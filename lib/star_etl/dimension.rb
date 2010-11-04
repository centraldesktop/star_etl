module StarEtl
  class Dimension
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
    
    def insert!
      @insert["pk_id"] = @insert.values.join
      sql = %Q{INSERT INTO #{name} (#{@insert.keys.join(", ")}) VALUES (#{prepare_values(@insert.values)});}
      begin
        Extractor.connection.execute(sql)    
      rescue ActiveRecord::StatementInvalid
      end
    end
    
    private
    
    def prepare_values(values)
      values.map do |v| 
        case v
        when String
          "'#{v}'"
        else
          v
        end
      end.join(", ")
    end
    
    def get_columns(table)
      cols = Extractor.connection.execute(%Q{select attname from pg_attribute where attrelid = (select oid from pg_class where relname = '#{table}') and attnum > 0})
      cols.map {|h| h["attname"] }
    end
    
  end
end