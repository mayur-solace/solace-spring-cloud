package com.solace.spring.cloud.stream.binder.inbound.acknowledge;

import com.solace.spring.cloud.stream.binder.util.SolaceAcknowledgmentException;
import org.springframework.integration.acks.AcknowledgmentCallback;

public class SolaceAckUtil {

  public static boolean isErrorQueueEnabled(AcknowledgmentCallback acknowledgmentCallback) {
    if (acknowledgmentCallback instanceof JCSMPAcknowledgementCallback jcsmpAcknowledgementCallback) {
      return jcsmpAcknowledgementCallback.isErrorQueueEnabled();
    } else if(acknowledgmentCallback instanceof JCSMPBatchAcknowledgementCallback batchAcknowledgementCallback) {
      return  batchAcknowledgementCallback.isErrorQueueEnabled();
    }

    return false;
  }

  public static boolean republishToErrorQueue(AcknowledgmentCallback acknowledgmentCallback) {
    if (!acknowledgmentCallback.isAcknowledged()
        && acknowledgmentCallback instanceof JCSMPAcknowledgementCallback jcsmpAcknowledgementCallback
        && jcsmpAcknowledgementCallback.isErrorQueueEnabled()) {
      try {
        if (jcsmpAcknowledgementCallback.republishToErrorQueue()) {
          return true;
        }
      } catch (Exception e) {
        throw new SolaceAcknowledgmentException(String.format("Failed to acknowledge XMLMessage %s",
            jcsmpAcknowledgementCallback.getMessageContainer().getMessage().getMessageId()), e);
      }
    } else if (!acknowledgmentCallback.isAcknowledged()
        && acknowledgmentCallback instanceof JCSMPBatchAcknowledgementCallback batchAcknowledgementCallback
        && batchAcknowledgementCallback.isErrorQueueEnabled()) {
      return batchAcknowledgementCallback.republishToErrorQueue();
    }

    return false;
  }
}