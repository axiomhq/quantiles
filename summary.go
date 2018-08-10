package quantiles

// SummaryEntry ...
type SummaryEntry struct {
	value   float64
	weight  float64
	minRank float64
	maxRank float64
}

// Equals ...
func (se SummaryEntry) Equals(o SummaryEntry) bool {
	return se == o
}

func (se SummaryEntry) prevMaxRank() float64 {
	return se.maxRank - se.weight
}

func (se SummaryEntry) nextMinRank() float64 {
	return se.minRank + se.weight
}

func compFn(a, b *SummaryEntry) bool {
	return a.value < b.value
}

// WeightedQuantilesSummary ...
type WeightedQuantilesSummary struct {
	entries []*SummaryEntry
}

// BuildFromBufferEntries ...
func (wqs *WeightedQuantilesSummary) BuildFromBufferEntries(bes []BufferEntry) {
	wqs.entries = []*SummaryEntry{}
	// TODO: entries_.reserve(buffer_entries.size());
	cumWeight := 0.0
	for _, entry := range bes {
		curWeight := entry.weight
		se := &SummaryEntry{
			value:   entry.value,
			weight:  entry.weight,
			minRank: cumWeight,
			maxRank: cumWeight + curWeight,
		}
		wqs.entries = append(wqs.entries, se)
		cumWeight += curWeight
	}
}

// BuildFromSummaryEntries ...
func (wqs *WeightedQuantilesSummary) BuildFromSummaryEntries(ses []*SummaryEntry) {
	wqs.entries = make([]*SummaryEntry, len(ses))
	// TODO: entries_.reserve(buffer_entries.size());
	for i, entry := range ses {
		wqs.entries[i] = entry
	}
}

// Merge ...
func (wqs *WeightedQuantilesSummary) Merge(other *WeightedQuantilesSummary) {
	otherEntries := other.entries
	if len(otherEntries) == 0 {
		return
	}
	if len(wqs.entries) == 0 {
		wqs.BuildFromSummaryEntries(other.entries)
		return
	}

	baseEntries := make([]*SummaryEntry, len(wqs.entries))
	for i, e := range wqs.entries {
		baseEntries[i] = e
	}
	wqs.entries = []*SummaryEntry{}
	// TODO: entries_.reserve(base_entries.size() + other_entries.size());

	// Merge entries maintaining ranks. The idea is to stack values
	// in order which we can do in linear time as the two summaries are
	// already sorted. We keep track of the next lower rank from either
	// summary and update it as we pop elements from the summaries.
	// We handle the special case when the next two elements from either
	// summary are equal, in which case we just merge the two elements
	// and simultaneously update both ranks.
	var (
		i            int
		j            int
		nextMinRank1 float64
		nextMinRank2 float64
	)

	for i != len(baseEntries) && j != len(otherEntries) {
		it1 := baseEntries[i]
		it2 := otherEntries[j]
		if it1.value < it2.value {
			wqs.entries = append(wqs.entries, &SummaryEntry{
				value: it1.value, weight: it1.weight,
				minRank: it1.minRank + nextMinRank2,
				maxRank: it1.maxRank + it2.prevMaxRank(),
			})
			nextMinRank1 = it1.nextMinRank()
			i++
		} else if it1.value > it2.value {
			wqs.entries = append(wqs.entries, &SummaryEntry{
				value: it2.value, weight: it2.weight,
				minRank: it2.minRank + nextMinRank1,
				maxRank: it2.maxRank + it1.prevMaxRank(),
			})
			nextMinRank2 = it2.nextMinRank()
			j++
		} else {
			wqs.entries = append(wqs.entries, &SummaryEntry{
				value: it1.value, weight: it1.weight + it2.weight,
				minRank: it1.minRank + it2.minRank,
				maxRank: it1.maxRank + it2.maxRank,
			})
			nextMinRank1 = it1.nextMinRank()
			nextMinRank2 = it2.nextMinRank()
			i++
			j++
		}
	}

	// Fill in any residual.
	for i != len(baseEntries) {
		it1 := baseEntries[i]
		wqs.entries = append(wqs.entries, &SummaryEntry{
			value: it1.value, weight: it1.weight,
			minRank: it1.minRank + nextMinRank2,
			maxRank: it1.maxRank + otherEntries[len(otherEntries)-1].maxRank,
		})
		i++
	}
	for j != len(otherEntries) {
		it2 := otherEntries[j]
		wqs.entries = append(wqs.entries, &SummaryEntry{
			value: it2.value, weight: it2.weight,
			minRank: it2.minRank + nextMinRank1,
			maxRank: it2.maxRank + baseEntries[len(baseEntries)-1].maxRank,
		})
		j++
	}
}

// Compress ...
func (wqs *WeightedQuantilesSummary) Compress(sizeHint int64, minEps float64) {
	// No-op if we're already within the size requirement.
	sizeHint = maxInt64(sizeHint, 2)
	if int64(len(wqs.entries)) <= sizeHint {
		return
	}

	// First compute the max error bound delta resulting from this compression.
	epsDelta := wqs.TotalWeight() * maxFloat64(1/float64(sizeHint), minEps)

	// Compress elements ensuring approximation bounds and elements diversity are both maintained.
	var (
		addAccumulator int64
		addStep        = int64(len(wqs.entries))
	)

	wi := 1
	li := wi

	for ri := 0; ri+1 != len(wqs.entries); {
		ni := ri + 1
		for ni != len(wqs.entries) && addAccumulator < addStep &&
			wqs.entries[ni].prevMaxRank()-wqs.entries[ri].nextMinRank() <= epsDelta {
			addAccumulator += sizeHint
			ni++
		}
		if wqs.entries[ri] == wqs.entries[ni-1] {
			ri++
		} else {
			ri = ni - 1
		}

		wqs.entries[wi] = wqs.entries[ri]
		wi++
		li = ri
		addAccumulator -= addStep
	}

	if li+1 != len(wqs.entries) {
		wqs.entries[wi] = wqs.entries[len(wqs.entries)-1]
		wi++
	}

	wqs.entries = wqs.entries[:wi]
}

// GenerateBoundaries ...
func (wqs *WeightedQuantilesSummary) GenerateBoundaries(numBoundaries int64) []float64 {
	output := []float64{}
	if len(wqs.entries) == 0 {
		return output
	}

	// Generate soft compressed summary.
	compressedSummary := &WeightedQuantilesSummary{}
	compressedSummary.BuildFromSummaryEntries(wqs.entries)
	// Set an epsilon for compression that's at most 1.0 / num_boundaries
	// more than epsilon of original our summary since the compression operation
	// adds ~1.0/num_boundaries to final approximation error.
	compressionEps := wqs.ApproximationError() + 1.0/float64(numBoundaries)
	compressedSummary.Compress(numBoundaries, compressionEps)

	// Return boundaries.
	for _, entry := range compressedSummary.entries {
		output = append(output, entry.value)
	}
	return output
}

// GenerateQuantiles ...
func (wqs *WeightedQuantilesSummary) GenerateQuantiles(numQuantiles int64) []float64 {
	output := []float64{}
	if len(wqs.entries) == 0 {
		return output
	}
	if numQuantiles < 2 {
		numQuantiles = 2
	}
	curIdx := 0
	for rank := 0.0; rank <= float64(numQuantiles); rank++ {
		d2 := 2 * (rank * wqs.entries[len(wqs.entries)-1].maxRank / float64(numQuantiles))
		nextIdx := curIdx + 1
		for nextIdx < len(wqs.entries) && d2 >= wqs.entries[nextIdx].minRank+wqs.entries[nextIdx].maxRank {
			nextIdx++
		}
		curIdx = nextIdx - 1
		// Determine insertion order.
		if nextIdx == len(wqs.entries) || d2 < wqs.entries[curIdx].nextMinRank()+wqs.entries[nextIdx].prevMaxRank() {
			output = append(output, wqs.entries[curIdx].value)
		} else {
			output = append(output, wqs.entries[nextIdx].value)
		}
	}
	return output
}

// ApproximationError ...
func (wqs *WeightedQuantilesSummary) ApproximationError() float64 {
	if len(wqs.entries) == 0 {
		return 0
	}

	var maxGap float64
	for i := 1; i < len(wqs.entries); i++ {
		it := wqs.entries[i]
		if tmp := it.maxRank - it.minRank - it.weight; tmp > maxGap {
			maxGap = tmp
		}
		if tmp := it.prevMaxRank() - wqs.entries[i-1].nextMinRank(); tmp > maxGap {
			maxGap = tmp
		}
	}
	return maxGap / wqs.TotalWeight()
}

// MinValue ...
func (wqs *WeightedQuantilesSummary) MinValue() float64 {
	if len(wqs.entries) != 0 {
		return wqs.entries[0].value
	}
	return 0
}

// MaxValue ...
func (wqs *WeightedQuantilesSummary) MaxValue() float64 {
	if len(wqs.entries) != 0 {
		return wqs.entries[len(wqs.entries)-1].value
	}
	return 0
}

// TotalWeight ...
func (wqs *WeightedQuantilesSummary) TotalWeight() float64 {
	if len(wqs.entries) != 0 {
		return wqs.entries[len(wqs.entries)-1].maxRank
	}
	return 0
}

// Size ...
func (wqs *WeightedQuantilesSummary) Size() int64 {
	return int64(len(wqs.entries))
}
