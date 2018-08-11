package quantiles

// sumEntry ...
type sumEntry struct {
	value   float64
	weight  float64
	minRank float64
	maxRank float64
}

func (se sumEntry) prevMaxRank() float64 {
	return se.maxRank - se.weight
}

func (se sumEntry) nextMinRank() float64 {
	return se.minRank + se.weight
}

func compFn(a, b *sumEntry) bool {
	return a.value < b.value
}

// Summary ...
type Summary struct {
	entries []*sumEntry
}

// newSummary ...
func newSummary() *Summary {
	return &Summary{
		entries: make([]*sumEntry, 0),
	}
}

// buildFromBufferEntries ...
func (sum *Summary) buildFromBufferEntries(bes []bufEntry) {
	sum.entries = []*sumEntry{}
	// TODO: entries_.reserve(buffer_entries.size());
	cumWeight := 0.0
	for _, entry := range bes {
		curWeight := entry.weight
		se := &sumEntry{
			value:   entry.value,
			weight:  entry.weight,
			minRank: cumWeight,
			maxRank: cumWeight + curWeight,
		}
		sum.entries = append(sum.entries, se)
		cumWeight += curWeight
	}
}

// BuildFromSummaryEntries ...
func (sum *Summary) BuildFromSummaryEntries(ses []*sumEntry) {
	sum.entries = make([]*sumEntry, len(ses))
	for i, entry := range ses {
		sum.entries[i] = entry
	}
}

// Merge ...
func (sum *Summary) Merge(other *Summary) {
	otherEntries := other.entries
	if len(otherEntries) == 0 {
		return
	}
	if len(sum.entries) == 0 {
		sum.BuildFromSummaryEntries(other.entries)
		return
	}

	baseEntries := make([]*sumEntry, len(sum.entries))
	for i, e := range sum.entries {
		baseEntries[i] = e
	}
	sum.entries = []*sumEntry{}

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
			sum.entries = append(sum.entries, &sumEntry{
				value: it1.value, weight: it1.weight,
				minRank: it1.minRank + nextMinRank2,
				maxRank: it1.maxRank + it2.prevMaxRank(),
			})
			nextMinRank1 = it1.nextMinRank()
			i++
		} else if it1.value > it2.value {
			sum.entries = append(sum.entries, &sumEntry{
				value: it2.value, weight: it2.weight,
				minRank: it2.minRank + nextMinRank1,
				maxRank: it2.maxRank + it1.prevMaxRank(),
			})
			nextMinRank2 = it2.nextMinRank()
			j++
		} else {
			sum.entries = append(sum.entries, &sumEntry{
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
		sum.entries = append(sum.entries, &sumEntry{
			value: it1.value, weight: it1.weight,
			minRank: it1.minRank + nextMinRank2,
			maxRank: it1.maxRank + otherEntries[len(otherEntries)-1].maxRank,
		})
		i++
	}
	for j != len(otherEntries) {
		it2 := otherEntries[j]
		sum.entries = append(sum.entries, &sumEntry{
			value: it2.value, weight: it2.weight,
			minRank: it2.minRank + nextMinRank1,
			maxRank: it2.maxRank + baseEntries[len(baseEntries)-1].maxRank,
		})
		j++
	}
}

// Compress ...
func (sum *Summary) Compress(sizeHint int64, minEps float64) {
	// No-op if we're already within the size requirement.
	sizeHint = maxInt64(sizeHint, 2)
	if int64(len(sum.entries)) <= sizeHint {
		return
	}

	// First compute the max error bound delta resulting from this compression.
	epsDelta := sum.TotalWeight() * maxFloat64(1/float64(sizeHint), minEps)

	// Compress elements ensuring approximation bounds and elements diversity are both maintained.
	var (
		addAccumulator int64
		addStep        = int64(len(sum.entries))
	)

	wi := 1
	li := wi

	for ri := 0; ri+1 != len(sum.entries); {
		ni := ri + 1
		for ni != len(sum.entries) && addAccumulator < addStep &&
			sum.entries[ni].prevMaxRank()-sum.entries[ri].nextMinRank() <= epsDelta {
			addAccumulator += sizeHint
			ni++
		}
		if sum.entries[ri] == sum.entries[ni-1] {
			ri++
		} else {
			ri = ni - 1
		}

		sum.entries[wi] = sum.entries[ri]
		wi++
		li = ri
		addAccumulator -= addStep
	}

	if li+1 != len(sum.entries) {
		sum.entries[wi] = sum.entries[len(sum.entries)-1]
		wi++
	}

	sum.entries = sum.entries[:wi]
}

// GenerateBoundaries ...
func (sum *Summary) GenerateBoundaries(numBoundaries int64) []float64 {
	// To construct the boundaries we first run a soft compress over a copy
	// of the summary and retrieve the values.
	// The resulting boundaries are guaranteed to both contain at least
	// num_boundaries unique elements and maintain approximation bounds.
	output := []float64{}
	if len(sum.entries) == 0 {
		return output
	}

	// Generate soft compressed summary.
	compressedSummary := &Summary{}
	compressedSummary.BuildFromSummaryEntries(sum.entries)
	// Set an epsilon for compression that's at most 1.0 / num_boundaries
	// more than epsilon of original our summary since the compression operation
	// adds ~1.0/num_boundaries to final approximation error.
	compressionEps := sum.ApproximationError() + 1.0/float64(numBoundaries)
	compressedSummary.Compress(numBoundaries, compressionEps)

	// Return boundaries.
	for _, entry := range compressedSummary.entries {
		output = append(output, entry.value)
	}
	return output
}

// GenerateQuantiles ...
func (sum *Summary) GenerateQuantiles(numQuantiles int64) []float64 {
	// To construct the desired n-quantiles we repetitively query n ranks from the
	// original summary. The following algorithm is an efficient cache-friendly
	// O(n) implementation of that idea which avoids the cost of the repetitive
	// full rank queries O(nlogn).
	output := []float64{}
	if len(sum.entries) == 0 {
		return output
	}
	if numQuantiles < 2 {
		numQuantiles = 2
	}
	curIdx := 0
	for rank := 0.0; rank <= float64(numQuantiles); rank++ {
		d2 := 2 * (rank * sum.entries[len(sum.entries)-1].maxRank / float64(numQuantiles))
		nextIdx := curIdx + 1
		for nextIdx < len(sum.entries) && d2 >= sum.entries[nextIdx].minRank+sum.entries[nextIdx].maxRank {
			nextIdx++
		}
		curIdx = nextIdx - 1
		// Determine insertion order.
		if nextIdx == len(sum.entries) || d2 < sum.entries[curIdx].nextMinRank()+sum.entries[nextIdx].prevMaxRank() {
			output = append(output, sum.entries[curIdx].value)
		} else {
			output = append(output, sum.entries[nextIdx].value)
		}
	}
	return output
}

// ApproximationError ...
func (sum *Summary) ApproximationError() float64 {
	if len(sum.entries) == 0 {
		return 0
	}

	var maxGap float64
	for i := 1; i < len(sum.entries); i++ {
		it := sum.entries[i]
		if tmp := it.maxRank - it.minRank - it.weight; tmp > maxGap {
			maxGap = tmp
		}
		if tmp := it.prevMaxRank() - sum.entries[i-1].nextMinRank(); tmp > maxGap {
			maxGap = tmp
		}
	}
	return maxGap / sum.TotalWeight()
}

// MinValue ...
func (sum *Summary) MinValue() float64 {
	if len(sum.entries) != 0 {
		return sum.entries[0].value
	}
	return 0
}

// MaxValue ...
func (sum *Summary) MaxValue() float64 {
	if len(sum.entries) != 0 {
		return sum.entries[len(sum.entries)-1].value
	}
	return 0
}

// TotalWeight ...
func (sum *Summary) TotalWeight() float64 {
	if len(sum.entries) != 0 {
		return sum.entries[len(sum.entries)-1].maxRank
	}
	return 0
}

// Size ...
func (sum *Summary) Size() int64 {
	return int64(len(sum.entries))
}

// Clear ...
func (sum *Summary) Clear() {
	sum.entries = []*sumEntry{}
}
