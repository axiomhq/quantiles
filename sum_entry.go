package quantiles

// SumEntry ...
type SumEntry struct {
	Value   float64
	Weight  float64
	MinRank float64
	MaxRank float64
}

func (se SumEntry) prevMaxRank() float64 {
	return se.MaxRank - se.Weight
}

func (se SumEntry) nextMinRank() float64 {
	return se.MinRank + se.Weight
}
