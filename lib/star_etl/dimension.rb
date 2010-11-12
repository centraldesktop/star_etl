module StarEtl
  class Dimension < Base
    
    class << self
      
      def columns
        @columns ||= {}
      end
      
    end
    
    attr_accessor :source_proc
    attr_reader :name, :source, :dest_cols
    
    def initialize(record)
      @record = record
      @insert = {}
    end
    
    def name=(name)
      @name      = name
      @dest_cols = get_columns(name)
    end
    
    def source=(col)
      s       = @record[col.to_s]      
      @source = source_proc.nil? ? s : source_proc.call(s)
    end
    
    def pk_id=(val)
      @insert["pk_id"] = val unless val.is_a?(Symbol)
      @pk_id = val
    end
    
    def pk_id
      ret = if @pk_id.is_a?(Symbol)
        @insert[@pk_id.to_s]
      else
        @pk_id
      end
      
      ret.zero? ? 'NULL' : ret
    end
    
    def method_missing(name, *args)
      col_name = name.to_s.gsub(/\=$/,'')
      if @dest_cols.include?(col_name)
        if args.size == 1
          @insert[col_name] = args.shift
        else
          @insert[col_name]
        end
      else
        super
      end
    end
    
    def columns
      @insert.keys.join(", ")
    end
    
    def insert_record
      @insert
      # result = sql(%Q{SELECT pk_id FROM #{name} WHERE pk_id = #{@insert["pk_id"]} })
      # %Q{(#{prepare_values(@insert.values)})} if result.empty?
    end
    
    def get_columns(table)
      return self.class.columns[table] if self.class.columns[table]
      cols    = sql(%Q{select attname from pg_attribute where attrelid = (select oid from pg_class where relname = '#{table}') and attnum > 0})
      col_map = cols.map {|h| h["attname"] }
      self.class.columns[table] = col_map
    end
    
  end
end