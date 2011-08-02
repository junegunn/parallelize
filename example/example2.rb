#!/usr/bin/env ruby

require 'rubygems'
require 'parallelize'

TOTAL_SIZE = 1_0000_0000
SLICE_SIZE = 100000

parallelize(4) do |thread_idx|
	cnt = 0 
	sst = Time.now
	(0...TOTAL_SIZE).each_slice(SLICE_SIZE) do |slice|
		st = Time.now
		slice.each do |i| 
			record = {}
			record['userid'] = rand(100_0000_0000).to_s(36)
			record['content'] = '_' * (50 + rand(100))
			record['reg_dttm'] = Time.now

			cnt += 1
		end 
		elapsed = Time.now - st
		total_elapsed = Time.now - sst 
		puts "#{thread_idx}: #{cnt} records upserted. #{"%.2f" % (SLICE_SIZE / elapsed)} records/sec. #{"%.2f" % (cnt / total_elapsed)} records/sec"
	end 
end 
