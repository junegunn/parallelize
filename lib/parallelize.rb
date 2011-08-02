require 'parallelize/parallel_exception'
require 'parallelize/enumerable_ext'

# Execute the given block with multiple threads.
# @return [Array] Threads.
# @param [Fixnum] n_thr Number of concurrent threads
# @param [Boolean] collect_exceptions If true, waits for all threads to complete even in case of exception, and throws ParallelException at the end. If false exception is immediately thrown.
def parallelize n_thr, collect_exceptions = false, &block
	n_thr.times.map.peach(n_thr, collect_exceptions, &block)
end

