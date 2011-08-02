require 'parallelize/parallel_exception'
require 'parallelize/enumerable_ext'

# Execute the given block with multiple threads.
# @return [Array] Threads.
# @param [Fixnum] num_threads Number of concurrent threads
# @param [Boolean] collect_exceptions If true, waits for all threads to complete even in case of exception, and throws ParallelException at the end. If false exception is immediately thrown.
def parallelize num_threads, collect_exceptions = false, &block
	num_threads.times.map.peach(num_threads, collect_exceptions, &block)
end

