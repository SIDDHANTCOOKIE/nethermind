// SPDX-FileCopyrightText: 2026 Demerzel Solutions Limited
// SPDX-License-Identifier: LGPL-3.0-only

using System;
using System.Buffers.Binary;
using System.Numerics;
using System.Threading;
using Nethermind.Core;

namespace Nethermind.Consensus.Processing;

/// <summary>
/// What block processing paid, in milliseconds, to execute the transactions no pre-warm run covered in time, as a moving
/// average per recipient and 4-byte selector; the block pre-warmer orders its warm runs by it.
/// </summary>
/// <remarks>
/// <para>
/// Bounded: the table is split into sets of <see cref="Ways"/> entries, and a key new to its set takes the place of the
/// entry of that set least recently learned or looked up.
/// </para>
/// <para>
/// Thread safety: entries are immutable and replaced whole, so a lookup takes no lock. A writer takes its set with a
/// single exchange and, when another writer holds it, drops the sample rather than wait; block processing, the one
/// writer, never blocks here.
/// </para>
/// </remarks>
internal sealed class TransactionCostTable
{
    public const int DefaultCapacity = 8192;
    internal const int Ways = 4;
    /// <summary>The weight of a new sample in the moving average.</summary>
    internal const double Smoothing = 0.25;
    // A 4-byte selector reads as 0 to uint.MaxValue, so a call with shorter data gets a key of its own.
    private const long NoSelector = -1;

    /// <summary>The table the block pre-warmers of the process share.</summary>
    public static TransactionCostTable Shared { get; } = new();

    private readonly Entry?[] _entries;
    private readonly int[] _lastUse;
    private readonly int[] _writers;
    private readonly int _setMask;
    private int _clock;
    private int _count;

    /// <param name="capacity">How many entries the table holds; a power of two of at least <see cref="Ways"/>.</param>
    public TransactionCostTable(int capacity = DefaultCapacity)
    {
        if (capacity < Ways || !BitOperations.IsPow2(capacity)) throw new ArgumentOutOfRangeException(nameof(capacity));
        _entries = new Entry?[capacity];
        _lastUse = new int[capacity];
        _writers = new int[capacity / Ways];
        _setMask = capacity / Ways - 1;
    }

    public int Count => Volatile.Read(ref _count);

    public int Capacity => _entries.Length;

    /// <summary>The learned cost of <paramref name="tx"/>; <c>false</c> for a contract creation or a key not learned.</summary>
    public bool TryGetCost(Transaction tx, out double costMs)
    {
        if (tx.To is not Address to)
        {
            costMs = 0;
            return false;
        }

        return TryGetCost(to, SelectorOf(tx), out costMs);
    }

    internal bool TryGetCost(Address to, long selector, out double costMs)
    {
        int first = SetOf(to, selector) * Ways;
        for (int slot = first; slot < first + Ways; slot++)
        {
            if (Volatile.Read(ref _entries[slot]) is { } entry && entry.Is(to, selector))
            {
                // A racy store: the recency only picks which entry a new key replaces.
                _lastUse[slot] = Volatile.Read(ref _clock);
                costMs = entry.CostMs;
                return true;
            }
        }

        costMs = 0;
        return false;
    }

    /// <summary>Folds what executing <paramref name="tx"/> took into the moving average of its key; a contract creation is not learned.</summary>
    public void Learn(Transaction tx, TimeSpan elapsed)
    {
        if (tx.To is not Address to) return;
        Learn(to, SelectorOf(tx), elapsed.TotalMilliseconds);
    }

    /// <returns><c>false</c> when another writer held the key's set and the sample was dropped.</returns>
    internal bool Learn(Address to, long selector, double costMs)
    {
        int set = SetOf(to, selector);
        if (Interlocked.CompareExchange(ref _writers[set], 1, 0) != 0) return false;

        try
        {
            int stamp = Interlocked.Increment(ref _clock);
            int first = set * Ways;
            int victim = first;
            for (int slot = first; slot < first + Ways; slot++)
            {
                Entry? entry = _entries[slot];
                if (entry is null)
                {
                    if (_entries[victim] is not null) victim = slot;
                    continue;
                }

                if (entry.Is(to, selector))
                {
                    Volatile.Write(ref _entries[slot], new Entry(to, selector, entry.CostMs + Smoothing * (costMs - entry.CostMs)));
                    _lastUse[slot] = stamp;
                    return true;
                }

                if (_entries[victim] is not null && _lastUse[slot] - _lastUse[victim] < 0) victim = slot;
            }

            if (_entries[victim] is null) Interlocked.Increment(ref _count);
            Volatile.Write(ref _entries[victim], new Entry(to, selector, costMs));
            _lastUse[victim] = stamp;
            return true;
        }
        finally
        {
            Volatile.Write(ref _writers[set], 0);
        }
    }

    internal static long SelectorOf(Transaction tx)
    {
        ReadOnlySpan<byte> data = tx.Data.Span;
        return data.Length >= 4 ? BinaryPrimitives.ReadUInt32BigEndian(data) : NoSelector;
    }

    private int SetOf(Address to, long selector)
    {
        uint hash = (uint)to.GetHashCode() ^ (uint)selector * 0x9E3779B1u;
        return (int)((hash ^ hash >> 16) & (uint)_setMask);
    }

    private sealed class Entry(Address to, long selector, double costMs)
    {
        private readonly Address _to = to;
        private readonly long _selector = selector;
        public readonly double CostMs = costMs;

        public bool Is(Address recipient, long key) => _selector == key && _to.Equals(recipient);
    }
}
