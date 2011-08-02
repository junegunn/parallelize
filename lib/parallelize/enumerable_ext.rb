module Enumerable
	# Divides the Enumerable objects into pieces and execute with multiple threads
	# @return [Array] Threads.
	# @param [Fixnum] n_thr Number of concurrent threads
	# @param [Boolean] collect_exceptions If true, waits for all threads to complete even in case of exception, and throws ParallelException at the end. If false exception is immediately thrown.
	def peach n_thr, collect_exceptions = false
		threads = []
		self.each_slice((self.count{true} / n_thr.to_f).ceil) do |slice|
			threads << 
				Thread.new(slice, threads.length) { |my_slice, thread_idx|
					my_slice.each { |e| yield e, thread_idx }
				}
		end

		exceptions = {}
		threads.each_with_index do |thr, idx|
			begin
				thr.join
			rescue Exception => e
				if collect_exceptions
					exceptions[idx] = e
				else
					raise e
				end
			end
		end

		if exceptions.empty?
			threads
		else
			raise ParallelException.new(exceptions)
		end
	end
end

