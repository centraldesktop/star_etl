# -*- encoding: utf-8 -*-
$:.push File.expand_path("../lib", __FILE__)
require "star_etl/version"

Gem::Specification.new do |s|
  s.name        = "star_etl"
  s.version     = StarEtl::VERSION
  s.platform    = Gem::Platform::RUBY
  s.authors     = ["Jon Druse"]
  s.email       = ["jon@jondruse.com"]
  s.homepage    = "http://rubygems.org/gems/star_etl"
  s.summary     = %q{StarEtl helps defining facts and dimensions for a star schema data-warehouse}
  s.description = %q{description is on it's way}

  s.rubyforge_project = "star_etl"

  s.files         = `git ls-files`.split("\n")
  s.test_files    = `git ls-files -- {test,spec,features}/*`.split("\n")
  s.executables   = `git ls-files -- bin/*`.split("\n").map{ |f| File.basename(f) }
  s.require_paths = ["lib"]
end
