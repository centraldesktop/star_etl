## 
# Author:: Jon Druse (mailto:jdruse@centraldesktop.com)
# 
# = Description
# 
# Base class just serves as a starting point.  Mainly to automatically include the helper module everywhere.
#
# == Change History
# 11/12/10:: added documentation Jon Druse (mailto:jdruse@centraldesktop.com)
### 
module StarEtl  
  class Base
    include StarEtl::Helper
  end
end
