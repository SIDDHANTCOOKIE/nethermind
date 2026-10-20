// SPDX-FileCopyrightText: 2026 Demerzel Solutions Limited
// SPDX-License-Identifier: LGPL-3.0-only

using System;
using System.Threading;
using System.Threading.Tasks;
using Nethermind.Consensus.Processing;
using Nethermind.Core;
using Nethermind.Core.Test.Builders;
using NUnit.Framework;

namespace Nethermind.Consensus.Test;

[Parallelizable(ParallelScope.All)]
public class TransactionCostTableTests
{
    [Test]
    public void A_key_starts_at_its_first_sample_and_moves_toward_each_later_one()
    {
        TransactionCostTable costs = new();
        Transaction tx = Call(TestItem.AddressA, [0xa9, 0x05, 0x9c, 0xbb, 0x01]);

        Assert.That(costs.TryGetCost(tx, out _), Is.False, "nothing learned yet");

        costs.Learn(tx, TimeSpan.FromMilliseconds(4));
        Assert.That(Cost(costs, tx), Is.EqualTo(4).Within(1e-9));

        costs.Learn(tx, TimeSpan.FromMilliseconds(8));
        Assert.That(Cost(costs, tx), Is.EqualTo(4 + TransactionCostTable.Smoothing * 4).Within(1e-9));
        Assert.That(costs.Count, Is.EqualTo(1));
    }

    [Test]
    public void Keys_are_the_recipient_and_the_first_four_bytes_of_the_call_data()
    {
        TransactionCostTable costs = new();
        costs.Learn(Call(TestItem.AddressA, [1, 2, 3, 4, 0xff]), TimeSpan.FromMilliseconds(3));
        costs.Learn(Call(TestItem.AddressA, [0x10]), TimeSpan.FromMilliseconds(7));
        costs.Learn(Build.A.Transaction.WithCode([0x00]).TestObject, TimeSpan.FromMilliseconds(11));

        using (Assert.EnterMultipleScope())
        {
            Assert.That(Cost(costs, Call(TestItem.AddressA, [1, 2, 3, 4])), Is.EqualTo(3).Within(1e-9), "the same selector, other arguments");
            Assert.That(costs.TryGetCost(Call(TestItem.AddressA, [1, 2, 3, 5]), out _), Is.False, "another selector");
            Assert.That(costs.TryGetCost(Call(TestItem.AddressB, [1, 2, 3, 4]), out _), Is.False, "another recipient");
            Assert.That(Cost(costs, Call(TestItem.AddressA, [])), Is.EqualTo(7).Within(1e-9), "calls without a selector share a key");
            Assert.That(costs.TryGetCost(Build.A.Transaction.WithCode([0x00]).TestObject, out _), Is.False, "a contract creation is not learned");
            Assert.That(costs.Count, Is.EqualTo(2));
        }
    }

    [Test]
    public void A_full_set_gives_up_its_least_recently_used_entry()
    {
        // One set: every key competes for the same ways.
        TransactionCostTable costs = new(TransactionCostTable.Ways);
        for (int key = 0; key < TransactionCostTable.Ways; key++) costs.Learn(TestItem.AddressA, key, key + 1);
        Assert.That(costs.TryGetCost(TestItem.AddressA, 0, out _), Is.True, "the oldest entry is used again");

        costs.Learn(TestItem.AddressA, TransactionCostTable.Ways, 9);

        using (Assert.EnterMultipleScope())
        {
            Assert.That(costs.TryGetCost(TestItem.AddressA, 1, out _), Is.False, "the least recently used entry made room");
            for (int key = 0; key <= TransactionCostTable.Ways; key++)
            {
                if (key != 1) Assert.That(costs.TryGetCost(TestItem.AddressA, key, out _), Is.True, $"key {key}");
            }

            Assert.That(costs.Count, Is.EqualTo(TransactionCostTable.Ways));
        }
    }

    [Test]
    public void The_table_never_holds_more_than_its_capacity([Values(4, 64, TransactionCostTable.DefaultCapacity)] int capacity)
    {
        TransactionCostTable costs = new(capacity);
        const int keys = 3 * TransactionCostTable.DefaultCapacity;
        for (int key = 0; key < keys; key++) costs.Learn(TestItem.AddressA, key, 1);

        using (Assert.EnterMultipleScope())
        {
            Assert.That(costs.Count, Is.InRange(1, capacity));
            Assert.That(costs.Capacity, Is.EqualTo(capacity));
            Assert.That(costs.TryGetCost(TestItem.AddressA, keys - 1, out _), Is.True, "the last key learned is kept");
        }
    }

    [TestCase(0)]
    [TestCase(2)]
    [TestCase(12)]
    public void A_capacity_must_be_a_power_of_two_of_at_least_one_set(int capacity) =>
        Assert.That(() => new TransactionCostTable(capacity), Throws.TypeOf<ArgumentOutOfRangeException>());

    [Test]
    [CancelAfter(60_000)]
    public void Concurrent_writers_and_readers_leave_every_entry_a_value_of_its_own_key(CancellationToken token)
    {
        // Each key is fed samples from a range of its own, so an average outside that range is a corrupted entry.
        const int keys = 96;
        const int writers = 6;
        const int samplesPerWriter = 20_000;
        TransactionCostTable costs = new(64);
        Address[] recipients = [TestItem.AddressA, TestItem.AddressB, TestItem.AddressC];
        int outOfRange = 0;
        using CancellationTokenSource writing = CancellationTokenSource.CreateLinkedTokenSource(token);

        Task[] readers = new Task[2];
        for (int i = 0; i < readers.Length; i++)
        {
            readers[i] = Task.Run(() =>
            {
                while (!writing.IsCancellationRequested)
                {
                    for (int key = 0; key < keys; key++)
                    {
                        if (costs.TryGetCost(recipients[key % recipients.Length], key, out double cost) && !InRange(key, cost)) Interlocked.Increment(ref outOfRange);
                    }
                }
            });
        }

        Parallel.For(0, writers, new ParallelOptions { MaxDegreeOfParallelism = writers, CancellationToken = token }, writer =>
        {
            Random random = new(writer);
            for (int i = 0; i < samplesPerWriter; i++)
            {
                int key = random.Next(keys);
                costs.Learn(recipients[key % recipients.Length], key, key * 10 + random.NextDouble());
            }
        });
        writing.Cancel();
        Task.WaitAll(readers, token);

        using (Assert.EnterMultipleScope())
        {
            Assert.That(outOfRange, Is.Zero);
            Assert.That(costs.Count, Is.InRange(1, costs.Capacity));
            for (int key = 0; key < keys; key++)
            {
                if (costs.TryGetCost(recipients[key % recipients.Length], key, out double cost)) Assert.That(InRange(key, cost), $"key {key} at {cost}");
            }

            int dropped = 0;
            for (int key = 0; key < 16 * costs.Capacity; key++)
            {
                if (!costs.Learn(TestItem.AddressD, key, 1)) dropped++;
            }

            Assert.That(dropped, Is.Zero, "a writer alone finds no set held");
        }

        static bool InRange(int key, double cost) => cost >= key * 10 && cost <= key * 10 + 1;
    }

    private static Transaction Call(Address to, byte[] data) => Build.A.Transaction.WithTo(to).WithData(data).TestObject;

    private static double Cost(TransactionCostTable costs, Transaction tx)
    {
        Assert.That(costs.TryGetCost(tx, out double cost), Is.True, "learned");
        return cost;
    }
}
