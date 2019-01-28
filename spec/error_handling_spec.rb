require 'spec_helper'

require 'json'
require 'thread'
require 'timeout'
require 'active_support/inflector/inflections'

RSpec.describe ActiveJob::GoogleCloudPubsub, :use_pubsub_emulator do
  class ErrorJob < ActiveJob::Base
    def perform(class_name)
      raise class_name.constantize, "Test #{class_name}"
    end
  end

  around :all do |example|
    orig, ActiveJob::Base.logger = ActiveJob::Base.logger, nil
    begin
      example.run
    ensure
      ActiveJob::Base.logger = orig
    end
  end

  shared_examples_for :error_handings do |on_error, expectation|
    it do
      args = {pubsub: Google::Cloud::Pubsub.new(emulator_host: @pubsub_emulator_host, project_id: 'activejob-test'), on_error: on_error}
      begin
        worker = ActiveJob::GoogleCloudPubsub::Worker.new(**args)

        worker.ensure_subscription

        thread = Thread.new {
          case on_error
          when 'acknowledge'
            expect_any_instance_of(Google::Cloud::Pubsub::ReceivedMessage).not_to receive(:reject!)
            expect_any_instance_of(Google::Cloud::Pubsub::ReceivedMessage).to receive(:acknowledge!).and_call_original
          when 'reject'
            expect_any_instance_of(Google::Cloud::Pubsub::ReceivedMessage).to receive(:reject!).and_call_original
            expect_any_instance_of(Google::Cloud::Pubsub::ReceivedMessage).not_to receive(:acknowledge!)
          when 'none'
            expect_any_instance_of(Google::Cloud::Pubsub::ReceivedMessage).not_to receive(:reject!)
            expect_any_instance_of(Google::Cloud::Pubsub::ReceivedMessage).not_to receive(:acknowledge!)
          end

          worker.run
        }

        thread.abort_on_exception = true

        ErrorJob.perform_later StandardError.name

      ensure
        thread.kill if thread
      end
    end
  end

  it_behaves_like :error_handings, 'acknowledge'
  it_behaves_like :error_handings, 'reject'
  it_behaves_like :error_handings, 'none'
end
