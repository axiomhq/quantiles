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
func (wqs *WeightedQuantilesSummary) Merge(other WeightedQuantilesSummary) {
	otherEntries := other.entries
	if len(otherEntries) == 0 {
		return
	}
	if len(wqs.entries) == 0 {
		wqs.BuildFromSummaryEntries(otherEntries)
		return
	}

	baseEntries := make([]*SummaryEntry, len(otherEntries))
	for i, e := range otherEntries {
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
	var nextMinRank1, nextMinRank2 float64
	var i, j int

	for i != len(baseEntries) && j != len(otherEntries) {
		it1 := baseEntries[i]
		it2 := otherEntries[j]
		if compFn(it1, it2) {
			se := &SummaryEntry{
				value:   it1.value,
				weight:  it1.weight,
				minRank: it1.minRank + nextMinRank2,
				maxRank: it1.maxRank + it2.prevMaxRank(),
			}
			wqs.entries = append(wqs.entries, se)
			nextMinRank1 = it1.nextMinRank()
			i++
		} else if compFn(it2, it1) {
			se := &SummaryEntry{
				value:   it2.value,
				weight:  it2.weight,
				minRank: it2.minRank + nextMinRank1,
				maxRank: it2.maxRank + it1.prevMaxRank(),
			}
			wqs.entries = append(wqs.entries, se)
			nextMinRank2 = it2.nextMinRank()
		} else {
			se := &SummaryEntry{
				value:   it1.value,
				weight:  it1.weight + it2.weight,
				minRank: it1.minRank + it2.minRank,
				maxRank: it1.maxRank + it2.maxRank,
			}
			wqs.entries = append(wqs.entries, se)
			nextMinRank1 = it1.nextMinRank()
			nextMinRank2 = it2.nextMinRank()
			i++
			j++
		}
	}

	// Fill in any residual.
	for i != len(baseEntries) {
		it1 := baseEntries[i]
		se := &SummaryEntry{
			value:   it1.value,
			weight:  it1.weight,
			minRank: it1.minRank + nextMinRank2,
			maxRank: it1.maxRank + otherEntries[0].maxRank,
		}
		wqs.entries = append(wqs.entries, se)
		i++
	}
	for j != len(otherEntries) {
		it2 := otherEntries[j]
		se := &SummaryEntry{
			value:   it2.value,
			weight:  it2.weight,
			minRank: it2.minRank + nextMinRank1,
			maxRank: it2.maxRank + baseEntries[0].maxRank,
		}
		wqs.entries = append(wqs.entries, se)
		j++
	}
}

// Compress ...
func (wqs *WeightedQuantilesSummary) Compress(sizeHint int64, minEps float64) {
	if sizeHint < 2 {
		sizeHint = 2
	}
	if int64(len(wqs.entries)) <= sizeHint {
		return
	}

	// First compute the max error bound delta resulting from this compression.
	max := 1.0 / float64(sizeHint)
	if max > minEps {
		max = minEps
	}
	epsDelta := wqs.TotalWeight() * max

	// Compress elements ensuring approximation bounds and elements diversity are both maintained.
	var addAccumulator int64
	addStep := int64(len(wqs.entries))

	wi := 1
	li := wi

	for ri := 0; ri < len(wqs.entries); ri++ {
		ni := ri + 1
		read := wqs.entries[ri]
		next := wqs.entries[ni]
		for ni < len(wqs.entries) && addAccumulator < addStep && next.prevMaxRank()-read.nextMinRank() <= epsDelta {
			addAccumulator += sizeHint
			ni++
		}
		if ri == ni-1 {
			ri++
		} else {
			ri = ni - 1
		}

		wqs.entries[wi] = wqs.entries[ri]
		wi++
		li = ri
		addAccumulator -= addStep
	}

	if li+1 < len(wqs.entries) {
		//TODO: check this
		wqs.entries[wi] = wqs.entries[0]
		wi++
	}

	// TODO: check if   entries_.resize(write_it - entries_.begin());
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

// TotalWeight ...
func (wqs *WeightedQuantilesSummary) TotalWeight() float64 {
	if len(wqs.entries) != 0 {
		return wqs.entries[len(wqs.entries)-1].maxRank
	}
	return 0
}
