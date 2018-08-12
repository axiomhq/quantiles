package quantiles

import (
	"math"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
)

type tuple [2]int64

func TestInvalidEps(t *testing.T) {
	assert := assert.New(t)
	_, _, err := getQuantileSpecs(-0.01, 0)
	assert.Error(err)
	_, _, err = getQuantileSpecs(1.01, 0)
	assert.Error(err)
}
func TestZeroEps(t *testing.T) {
	assert := assert.New(t)
	var (
		tup tuple
		err error
	)
	tup[0], tup[1], err = getQuantileSpecs(0, 0)
	assert.Error(err)
	tup[0], tup[1], err = getQuantileSpecs(0, 1)
	assert.Equal(tup, tuple{1, 2})
	tup[0], tup[1], err = getQuantileSpecs(0, 20)
	assert.Equal(tup, tuple{1, 20})
}
func TestNonZeroEps(t *testing.T) {
	assert := assert.New(t)
	var (
		tup tuple
		err error
	)
	tup[0], tup[1], err = getQuantileSpecs(0.01, 0)
	assert.Error(err)
	tup[0], tup[1], err = getQuantileSpecs(0.1, 320)
	assert.Equal(tup, tuple{4, 31})
	tup[0], tup[1], err = getQuantileSpecs(0.01, 25600)
	assert.Equal(tup, tuple{6, 501})
	tup[0], tup[1], err = getQuantileSpecs(0.01, 104857600)
	assert.Equal(tup, tuple{17, 1601})
	tup[0], tup[1], err = getQuantileSpecs(0.1, 104857600)
	assert.Equal(tup, tuple{20, 191})
	tup[0], tup[1], err = getQuantileSpecs(0.01, 1<<40)
	assert.Equal(tup, tuple{29, 2801})
	tup[0], tup[1], err = getQuantileSpecs(0.001, 1<<40)
	assert.Equal(tup, tuple{26, 25001})
}

func generateFixedUniformSummary(workerID int32, maxElements int64, totalWeight *float64, stream *Stream) error {
	for i := int64(0); i < maxElements; i++ {
		x := float64(i) / float64(maxElements)
		if err := stream.Push(x, 1); err != nil {
			return err
		}
		*totalWeight++
	}
	return stream.Finalize()
}

func generateRandUniformFixedWeightsSummary(workerID int32, maxElements int64, totalWeight *float64, stream *Stream) error {
	for i := int64(0); i < maxElements; i++ {
		x := rand.Float64()
		stream.Push(x, 1)
		*totalWeight++
	}
	return stream.Finalize()
}

func generateFixedNonUniformSummary(workerID int32, maxElements int64, totalWeight *float64, stream *Stream) error {
	for i := int64(0); i < maxElements; i++ {
		x := float64(i) / float64(maxElements)
		stream.Push(x, x)
		*totalWeight += x
	}
	return stream.Finalize()
}

func generateRandUniformRandWeightsSummary(workerID int32, maxElements int64, totalWeight *float64, stream *Stream) error {
	for i := int64(0); i < maxElements; i++ {
		x := rand.Float64()
		w := rand.Float64()
		stream.Push(x, w)
		*totalWeight += w
	}
	return stream.Finalize()
}

type workerSummaryGeneratorFunc func(int32, int64, *float64, *Stream) error

func testSingleWorkerStreams(t *testing.T, eps float64, maxElements int64,
	workerSummaryGenerator workerSummaryGeneratorFunc,
	expectedQuantiles []float64, quantilesMatcherEpsilon float64) {

	totalWeight := 0.0
	stream, err := New(eps, maxElements)
	if err != nil {
		t.Error("expected no error, got ", err)
		return
	}
	if err := workerSummaryGenerator(0, maxElements, &totalWeight, stream); err != nil {
		t.Error("expected no error, got ", err)
		return
	}

	// Ensure we didn't lose track of any elements and are
	// within approximation error bound.
	if val, err := stream.ApproximationError(0); err != nil {
		t.Error("expected no error, got ", err)
		return
	} else if val > eps {
		t.Errorf("expected val <= %v, got %v > %v", eps, val, eps)
		return
	}

	sum, err := stream.FinalSummary()
	if err != nil {
		t.Error("expected no error, got ", err)
		return
	}
	w := sum.TotalWeight()
	if math.Abs(totalWeight-w) > 1e-6 {
		t.Errorf("expected %v <= %v", math.Abs(totalWeight-w), 1e-6)
		return
	}

	// Verify expected quantiles.
	actuals, err := stream.GenerateQuantiles(int64(len(expectedQuantiles) - 1))
	if err != nil {
		t.Error("expected no error, got ", err)
		return
	}
	for i, eq := range expectedQuantiles {
		if val := math.Abs(actuals[i] - eq); val > quantilesMatcherEpsilon {
			t.Errorf("expected %v <= %v", val, quantilesMatcherEpsilon)
			return
		}
	}
}

// Stream generators.
func generateOneValue(workerID int32, maxElements int64, totalWeight *float64, stream *Stream) error {
	stream.Push(10, 1)
	*totalWeight++
	return stream.Finalize()
}

// Stream generators.
func generateOneZeroWeightedValue(workerID int32, maxElements int64, totalWeight *float64, stream *Stream) error {
	stream.Push(10, 0)
	return stream.Finalize()
}

func TestStreamOneValue(t *testing.T) {
	var (
		eps         = 0.01
		maxElements = int64(1 << 16)
	)
	testSingleWorkerStreams(t, eps, maxElements, generateOneValue,
		[]float64{10.0, 10.0, 10.0, 10.0, 10.0}, 1e-2)
}

func TestStreamOneZeroWeightValue(t *testing.T) {
	var (
		eps         = 0.01
		maxElements = int64(1 << 16)
	)
	testSingleWorkerStreams(t, eps, maxElements, generateOneZeroWeightedValue,
		[]float64{}, 1e-2)
}

func TestStreamFixedUniform(t *testing.T) {
	var (
		eps         = 0.01
		maxElements = int64(1 << 16)
	)
	testSingleWorkerStreams(t, eps, maxElements, generateFixedUniformSummary,
		[]float64{0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0}, 1e-2)
}

func TestStreamFixedNonUniform(t *testing.T) {
	var (
		eps         = 0.01
		maxElements = int64(1 << 16)
	)
	testSingleWorkerStreams(t, eps, maxElements, generateFixedNonUniformSummary,
		[]float64{0, math.Sqrt(0.1), math.Sqrt(0.2), math.Sqrt(0.3), math.Sqrt(0.4), math.Sqrt(0.5), math.Sqrt(0.6), math.Sqrt(0.7), math.Sqrt(0.8), math.Sqrt(0.9), 1.0}, 1e-2)
}

func TestStreamRandUniformFixedWeights(t *testing.T) {
	var (
		eps         = 0.01
		maxElements = int64(1 << 16)
	)
	testSingleWorkerStreams(t, eps, maxElements, generateRandUniformFixedWeightsSummary,
		[]float64{0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0}, 1e-2)
}

func TestStreamRandUniformRandWeights(t *testing.T) {
	var (
		eps         = 0.01
		maxElements = int64(1 << 16)
	)
	testSingleWorkerStreams(t, eps, maxElements, generateRandUniformRandWeightsSummary,
		[]float64{0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0}, 1e-2)
}

// Distributed tests.
func testDistributedStreams(t *testing.T, numWorkers int32, eps float64, maxElements int64,
	workerSummaryGenerator workerSummaryGeneratorFunc,
	expectedQuantiles []float64, quantilesMatcherEpsilon float64) {

	// Simulate streams on each worker running independently
	totalWeight := 0.0
	workerSummaries := [][]*SumEntry{}
	for i := int32(0); i < numWorkers; i++ {
		stream, err := New(eps/2, maxElements)
		if err != nil {
			t.Error("expected no error, got", err)
			return
		}
		workerSummaryGenerator(i, maxElements/int64(numWorkers), &totalWeight, stream)
		sum, err := stream.FinalSummary()
		if err != nil {
			t.Error("expected no error, got ", err)
			return
		}
		workerSummaries = append(workerSummaries, sum.entries)
	}

	// In the accumulation phase, we aggregate the summaries from each worker
	// and build an overall summary while maintaining error bounds by ensuring we
	// don't increase the error by more than eps / 2.
	reducerStream, err := New(eps, maxElements)
	if err != nil {
		t.Error("expected no error, got ", err)
		return
	}
	for _, summary := range workerSummaries {
		if err := reducerStream.PushSummary(summary); err != nil {
			t.Error("expected no error, got", err)
			return
		}
	}
	if err := reducerStream.Finalize(); err != nil {
		t.Error("expected no error, got", err)
		return
	}

	// Ensure we didn't lose track of any elements and are
	// within approximation error bound.
	if val, err := reducerStream.ApproximationError(0); err != nil {
		t.Error("expected no error, got ", err)
		return
	} else if val > eps {
		t.Errorf("expected val <= %v, got %v > %v", eps, val, eps)
		return
	}

	sum, err := reducerStream.FinalSummary()
	if err != nil {
		t.Error("expected no error, got ", err)
		return
	}
	w := sum.TotalWeight()
	if math.Abs(totalWeight-w) > totalWeight {
		t.Errorf("expected %v <= %v", math.Abs(totalWeight-w), totalWeight)
		return
	}

	// Verify expected quantiles.
	actuals, err := reducerStream.GenerateQuantiles(int64(len(expectedQuantiles) - 1))
	if err != nil {
		t.Error("expected no error, got ", err)
		return
	}
	for i, eq := range expectedQuantiles {
		if val := math.Abs(actuals[i] - eq); val > quantilesMatcherEpsilon {
			t.Errorf("expected %v <= %v", val, quantilesMatcherEpsilon)
			return
		}
	}
}

func TestStreamFixedUniformDistributed(t *testing.T) {
	var (
		numWorkers  int32 = 10
		eps               = 0.01
		maxElements       = int64(numWorkers) * int64(1<<16)
	)
	testDistributedStreams(t, numWorkers, eps, maxElements, generateFixedUniformSummary,
		[]float64{0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0}, 1e-2)
}

func TestStreamFixedNonUniformDistributed(t *testing.T) {
	var (
		numWorkers  int32 = 10
		eps               = 0.01
		maxElements       = int64(numWorkers) * int64(1<<16)
	)
	testDistributedStreams(t, numWorkers, eps, maxElements, generateFixedNonUniformSummary,
		[]float64{0, math.Sqrt(0.1), math.Sqrt(0.2), math.Sqrt(0.3), math.Sqrt(0.4), math.Sqrt(0.5), math.Sqrt(0.6), math.Sqrt(0.7), math.Sqrt(0.8), math.Sqrt(0.9), 1.0}, 1e-2)

}

func TestRandUniformFixedWeightsDistributed(t *testing.T) {
	var (
		numWorkers  int32 = 10
		eps               = 0.01
		maxElements       = int64(numWorkers) * int64(1<<16)
	)
	testDistributedStreams(t, numWorkers, eps, maxElements, generateRandUniformFixedWeightsSummary,
		[]float64{0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0}, 1e-2)
}

func TestRandUniformRandWeightsDistributed(t *testing.T) {
	var (
		numWorkers  int32 = 10
		eps               = 0.01
		maxElements       = int64(numWorkers) * int64(1<<16)
	)
	testDistributedStreams(t, numWorkers, eps, maxElements, generateRandUniformRandWeightsSummary,
		[]float64{0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0}, 1e-2)
}
