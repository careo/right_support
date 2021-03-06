require 'spec_helper'

describe RightSupport::Ruby::ObjectExtensions do
  context :require_succeeds? do
    it 'yields to its block when the require succeeds' do
      @canary = false

      # The 'set' source file ships with Ruby standard library and should
      # always be available
      if require_succeeds?('set')
        @canary = true
      end

      @canary.should == true
    end

    it 'does not yield when require fails for any reason' do
      @canary = false

      if require_succeeds?('a_source_file_with_a_wholly_improbable_name')
        @canary = true
      end

      @canary.should == false
    end
  end
end
