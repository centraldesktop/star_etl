module StarEtl
  module Helper
    
    def sql(q)
      Extractor.connection.execute(q)
    end
    
    def insert_record(table, record)
      sql(%Q{INSERT INTO #{table} (#{record.keys.join(", ")}) VALUES (#{prepare_values(record.values)});})
    end
    
    def prepare_values(values)
      values.map do |v|         
        if v.to_i.to_s == v
          v
        else
          "'#{v}'"
        end
      end.join(", ")
    end
    
  end
end