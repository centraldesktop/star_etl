## 
# Author:: Jon Druse (mailto:jdruse@centraldesktop.com)
# 
# = Description
# 
# The helper module.  Included in (almost) every class.  Create globally useful stuff here.
#
# == Change History
# 11/12/10:: added documentation Jon Druse (mailto:jdruse@centraldesktop.com)
### 
module StarEtl
  module Helper
    
    MUTEX   = Mutex.new
    
    ## 
    # Author:: Jon Druse (mailto:jdruse@centraldesktop.com)
    #
    # Runs +q+ on the connected database
    #
    # == Parameters
    #
    # * +q+ - String - the actual sql query to run
    #
    # == Returns
    #
    # The array of records returned
    #
    # == Change History
    # 11/12/10:: added documentation Jon Druse (mailto:jdruse@centraldesktop.com)
    ### 
    def sql(q)
      StarEtl.connection.execute(q)
    end
    
    ## 
    # Author:: Jon Druse (mailto:jdruse@centraldesktop.com)
    #
    # == Parameters
    #
    # * +table+ - String - the name of the table to insert the +record+ into
    # * +record+ - Hash - the hash representation of the record to insert
    #
    # == Returns
    #
    # String - The number of records inserted
    #
    # == Change History
    # 11/12/10:: added documentation Jon Druse (mailto:jdruse@centraldesktop.com)
    ### 
    def insert_record(table, record)
      record.delete_if { |key, val| val.nil? || val == "" }
      # this guarantees that the cols and values are in the same order
      a          = record.to_a
      cols, vals = a.map(&:shift), a.map(&:shift)      
      sql(%Q{INSERT INTO #{table} (#{cols.join(", ")}) VALUES (#{prepare_values(vals)});})
    end
    
    ## 
    # Author:: Jon Druse (mailto:jdruse@centraldesktop.com)
    #
    # If the debug option is set to true (it's false by default) this just puts the message to STDOUT
    #
    # == Parameters
    #
    # * +msg+ - String - the message to print
    #
    # == Change History
    # 11/12/10:: added documentation Jon Druse (mailto:jdruse@centraldesktop.com)
    ### 
    def debug(msg)
      puts msg if StarEtl::Extractor.options[:debug]
    end
    
    ## 
    # Author:: Jon Druse (mailto:jdruse@centraldesktop.com)
    #
    # Insert statements can be finiky when it comes to values.  This method puts '' around strings and removes them from integers.
    #
    # == Parameters
    #
    # * +values+ - Array - the values to prepare for a sql insert.
    #
    # == Returns
    #
    # String - a string fit for the VALUES sections of a sql insert statement
    #
    # == Change History
    # 11/12/10:: added documentation Jon Druse (mailto:jdruse@centraldesktop.com)
    ### 
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
    
    ## 
    # Author:: Jon Druse (mailto:jdruse@centraldesktop.com)
    #
    # This method rounds a time stamp down to the start of the minute.
    #
    # == Parameters
    #
    # * +stamp+ - Int - A time stamp
    #
    # == Returns
    #
    # Int - A timestamp 
    #
    # == Change History
    # 11/12/10:: added documentation Jon Druse (mailto:jdruse@centraldesktop.com)
    ### 
    def round_down_to_minute(stamp)
      (stamp.to_f / 60).floor * 60
    end

    def round_down_to_hour(stamp)
      (stamp.to_f / 3600).floor * 3600
    end

    
  end
end