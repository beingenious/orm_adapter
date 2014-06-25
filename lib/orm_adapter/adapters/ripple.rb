require 'ripple'

module Ripple
  module Document
    module ClassMethods
      include OrmAdapter::ToAdapter
    end

    class OrmAdapter < ::OrmAdapter::Base
      # get a list of column names for a given class
      def column_names
        klass.properties.keys
      end

      # @see OrmAdapter::Base#get!
      def get!(id)
        klass.find(wrap_key(id))
      end

      # @see OrmAdapter::Base#get
      def get(id)
        klass.find(wrap_key(id))
      end

      # @see OrmAdapter::Base#find_first
      def find_first(options = {})
        conditions, order = extract_conditions!(options)
        klass.find(keys_for_conditions(conditions).first)
      end

      # @see OrmAdapter::Base#find_all
      def find_all(options = {})
        # conditions, order, limit, offset = extract_conditions!(options)
        # not called by devise, but implementation can be made with Riak::SecondaryIndex and max_results
      end

      # @see OrmAdapter::Base#create!
      def create!(attributes = {})
        klass.create!(attributes)
      end

      # @see OrmAdapter::Base#destroy
      def destroy(object)
        object.destroy if valid_object?(object)
      end

    protected

      def keys_for_conditions(conditions)
        index2i = conditions.find do |k, v|
          !klass.indexes[k].nil?
        end
        map = "
          function(v) {
            if (v.values) {
              original = v;
              var v = Riak.mapValuesJson(v)[0];
              return (#{conditions.map { |k,v| "v.#{k} === '#{v}'" }.join(' && ')}) ? [decodeURIComponent(original.key)] : [];
            } else return [];
          }
        "
        if index2i
          Riak::MapReduce.new(klass.bucket.client).index(klass.bucket, klass.indexes[index2i[0]].index_key,
            index2i[1]).map(map, :keep => true).run
        else
          Riak::MapReduce.new(klass.bucket.client).add(klass.bucket).map(map, :keep => true).run
        end
      end

    end
  end
end
