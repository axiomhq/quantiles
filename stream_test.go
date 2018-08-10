package quantiles

import (
	"fmt"
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

type WeightedQuantilesStreamDummy WeightedQuantilesStream

func generateFixedUniformSummary(workerID int32, maxElements int64, totalWeight *float64, stream *WeightedQuantilesStream) error {
	for i := int64(0); i < maxElements; i++ {
		x := float64(i) / float64(maxElements)
		stream.PushEntry(x, 1)
		*totalWeight++
	}
	return stream.Finalize()
}

func generateFixedNonUniformSummary(workerID int32, maxElements int64, totalWeight *float64, stream *WeightedQuantilesStream) error {
	for i := int64(0); i < maxElements; i++ {
		x := float64(i) / float64(maxElements)
		stream.PushEntry(x, x)
		*totalWeight += x
	}
	return stream.Finalize()
}

func generateRandUniformFixedWeightsSummary(workerID int32, maxElements int64, totalWeight *float64, stream *WeightedQuantilesStream) error {
	for i := int64(0); i < maxElements; i++ {
		x := rand.Float64()
		stream.PushEntry(x, 1)
		*totalWeight++
	}
	return stream.Finalize()
}

func generateRandUniformRandWeightsSummary(workerID int32, maxElements int64, totalWeight *float64, stream *WeightedQuantilesStream) error {
	for i := int64(0); i < maxElements; i++ {
		x := rand.Float64()
		w := rand.Float64()
		stream.PushEntry(x, w)
		*totalWeight += w
	}
	return stream.Finalize()
}

type workerSummaryGeneratorFunc func(int32, int64, *float64, *WeightedQuantilesStream) error

func testSingleWorkerStreams(t *testing.T, eps float64, maxElements int64,
	workerSummaryGenerator workerSummaryGeneratorFunc,
	expectedQuantiles []float64, quantilesMatcherEpsilon float64) {

	totalWeight := 0.0
	stream, err := NewWeightedQuantilesStream(eps, maxElements)
	if err != nil {
		t.Error("expected no error, got ", err)
		return
	}
	workerSummaryGenerator(0, maxElements, &totalWeight, stream)

	// Ensure we didn't lose track of any elements and are
	// within approximation error bound.
	if val, err := stream.ApproximationError(0); err != nil {
		t.Error("expected no error, got ", err)
		return
	} else if val > eps {
		t.Errorf("expected val <= %v, got %v > %v", eps, val, eps)
		return
	}

	sum, err := stream.GetFinalSummary()
	if err != nil {
		t.Error("expected no error, got ", err)
		return
	}
	w := sum.TotalWeight()
	if math.Abs(totalWeight-w) > 1e-6 {
		t.Errorf("expected %v <= %v", math.Abs(totalWeight-w), 1e-6)
		//return
	}

	// Verify expected quantiles.
	actuals, err := stream.GenerateQuantiles(int64(len(expectedQuantiles) - 1))
	if err != nil {
		t.Error("expected no error, got ", err)
		return
	}
	for i, eq := range expectedQuantiles {
		fmt.Println(i, actuals[i], eq, quantilesMatcherEpsilon)
		if val := math.Abs(actuals[i] - eq); val > quantilesMatcherEpsilon {
			t.Errorf("expected %v <= %v", val, quantilesMatcherEpsilon)
			//return
		}
	}
}

// Stream generators.
func generateOneValue(workerID int32, maxElements int64, totalWeight *float64, stream *WeightedQuantilesStream) error {
	stream.PushEntry(10, 1)
	*totalWeight++
	return stream.Finalize()
}

// Stream generators.
func generateOneZeroWeightedValue(workerID int32, maxElements int64, totalWeight *float64, stream *WeightedQuantilesStream) error {
	stream.PushEntry(10, 0)
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

func TestStreamRandUniformRandWeights(t *testing.T) {
	var (
		eps         = 0.01
		maxElements = int64(1 << 16)
	)
	testSingleWorkerStreams(t, eps, maxElements, generateRandUniformRandWeightsSummary,
		[]float64{0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0}, 1e-2)
}
