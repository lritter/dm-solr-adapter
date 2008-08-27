require 'rubygems'
gem 'dm-core', '>=0.9.2'
require 'dm-core'
gem 'solr-ruby', '>=0.0.6'
require 'solr'

# (lritter 21/08/2008 15:15): TODO Make :serial => true properties that are empty get a UUID?  hmm.  type-wise
# this might not work out.

module DataMapper
  module Resource
    def to_solr_document(dirty=false)
      property_list = self.class.properties.select { |key, value| dirty ? self.dirty_attributes.key?(key) : true }
      inferred_fields = {:type => solr_type_name}
      return Solr::Document.new(property_list.inject(inferred_fields) do |accumulator, property|
        if(value = attribute_get(property.name))
          cast_value = value
          if(property.type.respond_to?(:dump))
            cast_value = property.type.dump(value, property)
          end
          
          if value.kind_of?(Date) 
            cast_value = value.strftime('%Y-%m-%dT%H:%M:%SZ')
          end
          
          # puts "Cast #{property.name}:#{property.type} from '#{value.inspect}' to '#{cast_value.inspect}'"
          
          accumulator[property.field] = cast_value
        end
        accumulator
      end)
    end
    
    protected
    def solr_type_name
      self.class.name.downcase
    end
  end

end

module DataMapper
  module Adapters
    class SolrAdapter < AbstractAdapter
      
      def create(resources)
        created = 0
        with_connection do |connection|
          if(connection.add(resources.map{|r| r.to_solr_document}))
            created += 1
          end
        end
        
        created
      end

      def read_many(query)
        results = with_connection do |connection|
          connection.query(*build_request(query))
        end
        convert_solr_results_to_collection(query, results)
      end

      def read_one(query)
        results = with_connection do |connection|
          request = build_request(query, :start => 0, :rows => 1)
          connection.query(*request)
        end
        unless(results.total_hits == 0)
          data = results.hits.first
          resource = query.model.load(query.fields.map { |property| 
            property.typecast(data[property.field.to_s]) 
          }, query)
          set_solr_score_from_solr_result(resource, data)
          resource
        end
      end

      def update(attributes, query)
        updated = 0
        resources = read_many(query)
        
        resources.each do |resource| 
          updated +=1 if with_connection do |connection|
            connection.update(resource.to_solr_document)
          end 
        end
        updated
      end

      def delete(query)
        deleted = 0
        deleted += 1 if with_connection do |connection|
          connection.delete_by_query(build_request(query).first)
        end

        deleted
      end
      
      protected
      attr_accessor :solr_connection
      
      # Converts the URI's scheme into a parsed HTTP identifier.
      def normalize_uri(uri_or_options)
        if String === uri_or_options
          uri_or_options = Addressable::URI.parse(uri_or_options)
        end
        if Addressable::URI === uri_or_options
          return uri_or_options.normalize
        end

        adapter = uri_or_options.delete(:adapter)
        user = nil
        password = nil
        host = (uri_or_options.delete(:host) || "")
        port = uri_or_options.delete(:port)
        index = uri_or_options.delete(:index)
        query = nil

        normalized = Addressable::URI.new(
          adapter, user, password, host, port, index, query, nil
        )
        
        return normalized
      end

      # (lritter 27/08/2008 13:39): will only return up to 100000 records...
      # it does not seem possible to tell solr to return all records.      
      def build_request(query, options={})
        # puts query.inspect
        query_fragments = []
        query_fragments << "+type:#{query.model.name.downcase}" # (lritter 13/08/2008 09:54): This should be be factored into a method
        
        options.merge!(:rows => (query.limit || 100000))
        options.merge!(:start => query.offset) if query.offset
        
        query_fragments += query.conditions.map { |operator, property, value|
          field = "#{property.field}:"
          case operator
          when :eql   then "+#{field}#{format_value_for_conditions(operator, property, value)}"
          when :not   then "-#{field}#{value}"
          when :gt    then "+#{field}{#{value} TO *}"
          when :gte   then "+#{field}[#{value} TO *]"
          when :lt    then "+#{field}{* TO #{value}}"
          when :lte   then "+#{field}[* TO #{value}]"
          when :in    then "+#{field}(#{value.join(' ')})"
          when :like  then "+#{field}#{value.gsub('%','*')}"
          end
        }
        
        order_fragments = query.order.map do |order|
          {order.property.field => (order.direction == :asc ? :ascending : :descending)}
        end
  
        options.merge!(:sort => order_fragments) unless order_fragments.empty?
      
        [query_fragments.join(' '), options]
      end
      
      def format_value_for_conditions(operator, property, value)
        value.kind_of?(Enumerable) ? "(#{value.to_a.join(' ')})" : value
      end
      
      def solr_commit
        with_connection(false) { |c| c.commit }
      end
      
      def with_connection(autocommit = true, &block)
        connection = nil
        begin
          connection = create_connection
          result = block.call(connection)
          solr_commit if autocommit
          return result
        rescue => e
          # (lritter 12/08/2008 16:48): Loggger?
          puts e.to_s
          puts e.backtrace.join("\n")
          raise e
        ensure
          destroy_connection(connection)
        end
      end
      
      def create_connection
        connect_to = uri.dup 
        connect_to.scheme = 'http'
        Solr::Connection.new(connect_to.to_s, :autocommit => :off)
      end
      
      def destroy_connection(connection)
        connection = nil
      end
      
      def convert_solr_results_to_collection(query, results)
        Collection.new(query) do |collection|
          results.hits.each do |data|
            resource = collection.load(
              query.fields.map { |property| property.typecast(data[property.field.to_s]) }
            )
            set_solr_score_from_solr_result(resource, data)
            resource
          end
        end
      end
      
      def set_solr_score_from_solr_result(resource, solr_data)
        resource.instance_eval "def score; #{solr_data['score']}; end"
      end
      
      def log
        $stderr
      end
    end
  end

  module Model
    def solr_type_name
      name.downcase
    end
    
    def solr_type_restriction
      "+type:#{solr_type_name}"
    end
    
    def search_by_solr(*args)
      repository = repository(default_repository_name)
      options = args.last.is_a?(Hash) ? args.pop : {}
      query = args.shift || ''
      query = "#{solr_type_restriction} #{query}"
      
      results = repository.adapter.send(:with_connection) do |connection|
        connection.query(query, options)
      end
      repository.adapter.send(:convert_solr_results_to_collection, Query.new(repository, self), results)
    end
    
    def random_set(*args)
      options = args.last.is_a?(Hash) ? args.pop : {}
      query = args.shift || ''
      options.merge!({:sort => [{"random_#{rand(9999)}".to_sym => :ascending}]})
      search_by_solr(query, options)
    end
    
    def create_many(enumerable, options = {}, &factory)
      options[:factory] = factory
      in_batches(enumerable, options) do |batch|
        repository.adapter.create(batch)
      end
    end
    
    def in_batches(enumerable, options = {}, &batch_operation)
      raise "Batch operation must be specified" unless batch_operation
      default_options = { 
        :batch_size => 100,
        :factory => lambda {|x| x},
        :continue_on_errors => true
      }
      options = default_options.merge(options)
      
      factory = options[:factory]
      batch_size = options[:batch_size]
      continue_on_errors = options[:continue_on_errors]
      inputs_that_could_not_be_loaded = []
      items_that_could_not_be_created = []
      batch = []
      
      enumerable.each_with_index do |input, record_number|
        current_batch = record_number/batch_size
        
        # log.puts "Item: #{input}"
        
        begin # Try and load this input and add it to the batch
          processed_item = factory.call(input)
          batch << processed_item if processed_item
        rescue => e
          throw e unless continue_on_errors
          inputs_that_could_not_be_loaded << input
          log.puts "Could not load input: #{input.inspect}"
          log.puts e.to_s
          log.puts e.backtrace.join("\n")
        end
        
        if batch_size <= batch.size # Time to commit this batch
          (items_that_could_not_be_created += with_batch(batch, !continue_on_errors, &batch_operation)) and (batch = [])
        end
      end
      
      unless batch.empty?
        (items_that_could_not_be_created += with_batch(batch, !continue_on_errors, &batch_operation)) and (batch = [])
        # items_that_could_not_be_created += with_batch(batch, !continue_on_errors, &batch_operation) 
      end
      
      if !items_that_could_not_be_created.empty? 
        problem_outputs = []
        if 1 < batch_size
          recursive_options = options.dup
          recursive_options[:batch_size] = recursive_options[:batch_size]/2
          recursive_options[:factory] = lambda {|x| x}
          problem_outputs = in_batches(items_that_could_not_be_created, recursive_options, &batch_operation)
        else
          items_that_could_not_be_created.each do |item|
            problem_outputs += with_batch([item], !continue_on_errors, &batch_operation)
          end
        end
      end
      
      problem_outputs
    end
    
    protected
    
    def log
      repository.adapter.send(:log)
    end
    
    def with_batch(batch, noisy = true, &op)
      outputs_that_could_not_be_created = []
      begin # try and add the batch
        # log.print "Doing: #{batch.inspect}..."
        op.call(batch)
        repository.adapter.send(:solr_commit)
        # log.puts "Done"
      rescue => e
        throw e if noisy
        outputs_that_could_not_be_created += batch
        log.puts e.to_s
        log.puts e.backtrace.join("\n")
      ensure
        batch = []
      end
      # puts "Sending back: #{outputs_that_could_not_be_created.inspect}"
      return outputs_that_could_not_be_created
    end

  end
end