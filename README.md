# quantiles - Optimal Quantile Approximation in Streams

This is a translation of [TensorFlow's quantile helper class](https://github.com/tensorflow/tensorflow/tree/master/tensorflow/contrib/boosted_trees/lib/quantiles), it aims to compute approximate quantiles with error bound guarantees for weighted data sets.
This implementation is an adaptation of techniques from the following papers:
* (2001) [Space-efficient online computation of quantile summaries](http://infolab.stanford.edu/~datar/courses/cs361a/papers/quantiles.pdf).
* (2004) [Power-conserving computation of order-statistics over sensor networks](http://www.cis.upenn.edu/~mbgreen/papers/pods04.pdf).
* (2007) [A fast algorithm for approximate quantiles in high speed data streams](http://web.cs.ucla.edu/~weiwang/paper/SSDBM07_2.pdf).
* (2016) [XGBoost: A Scalable Tree Boosting System](https://arxiv.org/pdf/1603.02754.pdf).

### The key ideas at play are the following:
* Maintain an in-memory multi-level quantile summary in a way to guarantee
  a maximum approximation error of eps * W per bucket where W is the total
  weight across all points in the input dataset.
* Two base operations are defined: `MERGE` and `COMPRESS`. `MERGE` combines two
  summaries guaranteeing a `epsNew = max(eps1, eps2)`. `COMPRESS` compresses
  a summary to `b + 1` elements guaranteeing `epsNew = epsOld + 1/b`.
* b * sizeof(summary entry) must ideally be small enough to fit in an
  average CPU L2 cache.
* To distribute this algorithm with maintaining error bounds, we need
  the worker-computed summaries to have no more than `eps / h` error
  where h is the height of the distributed computation graph which
  is 2 for an MR with no combiner.

We mainly want to max out IO bw by ensuring we're not compute-bound and
using a reasonable amount of RAM.

### Complexity:
* Compute: `O(n * log(1/eps * log(eps * n)))`.
* Memory: `O(1/eps * log^2(eps * n))` <- for one worker streaming through the
                                     entire dataset.

An epsilon value of zero would make the algorithm extremely inefficent and
therefore, is disallowed.

