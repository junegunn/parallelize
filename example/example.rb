#!/usr/bin/env ruby

require 'rubygems'
require 'parallelize'

parallelize(4) do
	puts "I'm a thread"
end

parallelize(4) do |thread_idx|
	puts "I'm thread ##{thread_idx}" # thread_idx is zero-based
	# ...
end

# Enumerable#peach
(0..100).peach(4) do |elem, thread_idx|
	puts "Thread ##{thread_idx} processing #{elem}"
end

begin
	parallelize(4, true) do |elem, thread_idx|
		# Each thread can complete its block even when some other threads throw exceptions
		raise Exception.new(thread_idx) if thread_idx < 2
		sleep 3
	end
rescue ParallelException => e
	p e.exceptions
end

