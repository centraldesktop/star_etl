module StarEtl
  class Dimension < Base
    
    attr_accessor :source_proc
    attr_reader :name, :source
    
    def initialize(record)
      @record = record
      @insert = {}
    end
    
    def name=(name)
      @name = name
      @dest_cols = get_columns(name)
    end
    
    def source=(col)
      s = @record[col.to_s]      
      @source = source_proc.nil? ? s : source_proc.call(s)
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
    
    def insert_values
      %Q{(#{prepare_values(@insert.values)})} unless @skip
    end
    
    def pk
      begin
        @skip = sql(%Q{SELECT * FROM #{name} WHERE pk_id = #{@insert["pk_id"]} }).size > 0
        @insert["pk_id"]
      rescue ActiveRecord::StatementInvalid => e
        raise e unless e.to_s.include?('duplicate key')
      end
    end
    
    private
    
    def get_columns(table)
      cols = sql(%Q{select attname from pg_attribute where attrelid = (select oid from pg_class where relname = '#{table}') and attnum > 0})
      cols.map {|h| h["attname"] }
    end
    
  end
end