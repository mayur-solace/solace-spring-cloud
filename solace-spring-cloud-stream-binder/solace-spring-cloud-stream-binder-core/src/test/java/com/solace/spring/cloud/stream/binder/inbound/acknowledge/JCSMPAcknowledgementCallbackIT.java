package com.solace.spring.cloud.stream.binder.inbound.acknowledge;

import static com.solace.spring.cloud.stream.binder.test.util.RetryableAssertions.retryAssert;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import com.solace.spring.boot.autoconfigure.SolaceJavaAutoConfiguration;
import com.solace.spring.cloud.stream.binder.properties.SolaceConsumerProperties;
import com.solace.spring.cloud.stream.binder.util.ErrorQueueInfrastructure;
import com.solace.spring.cloud.stream.binder.util.FlowReceiverContainer;
import com.solace.spring.cloud.stream.binder.util.JCSMPSessionProducerManager;
import com.solace.spring.cloud.stream.binder.util.MessageContainer;
import com.solace.spring.cloud.stream.binder.util.SolaceAcknowledgmentException;
import com.solace.spring.cloud.stream.binder.util.SolaceBatchAcknowledgementException;
import com.solace.spring.cloud.stream.binder.util.SolaceStaleMessageException;
import com.solace.spring.cloud.stream.binder.util.UnboundFlowReceiverContainerException;
import com.solace.test.integration.junit.jupiter.extension.ExecutorServiceExtension;
import com.solace.test.integration.junit.jupiter.extension.PubSubPlusExtension;
import com.solace.test.integration.semp.v2.SempV2Api;
import com.solace.test.integration.semp.v2.config.model.ConfigMsgVpnQueue;
import com.solace.test.integration.semp.v2.monitor.model.MonitorMsgVpnQueueMsg;
import com.solace.test.integration.semp.v2.monitor.model.MonitorMsgVpnQueueMsgsResponse;
import com.solace.test.integration.semp.v2.monitor.model.MonitorSempMeta;
import com.solace.test.integration.semp.v2.monitor.model.MonitorSempPaging;
import com.solacesystems.jcsmp.EndpointProperties;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.Queue;
import com.solacesystems.jcsmp.TextMessage;
import com.solacesystems.jcsmp.XMLMessageProducer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.function.ThrowingRunnable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junitpioneer.jupiter.cartesian.CartesianTest;
import org.junitpioneer.jupiter.cartesian.CartesianTest.Values;
import org.mockito.Mockito;
import org.springframework.boot.test.context.ConfigDataApplicationContextInitializer;
import org.springframework.integration.acks.AcknowledgmentCallback;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

@SpringJUnitConfig(classes = SolaceJavaAutoConfiguration.class,
		initializers = ConfigDataApplicationContextInitializer.class)
@ExtendWith(ExecutorServiceExtension.class)
@ExtendWith(PubSubPlusExtension.class)
@Timeout(value = 2, unit = TimeUnit.MINUTES)
public class JCSMPAcknowledgementCallbackIT {
	private final AtomicReference<FlowReceiverContainer> flowReceiverContainerReference = new AtomicReference<>();
	private XMLMessageProducer producer;
	private String vpnName;
	private Runnable closeErrorQueueInfrastructureCallback;

	private static final Log logger = LogFactory.getLog(JCSMPAcknowledgementCallbackIT.class);

	@BeforeEach
	public void setup(JCSMPSession jcsmpSession) throws Exception {
		producer = jcsmpSession.getMessageProducer(new JCSMPSessionProducerManager.CloudStreamEventHandler());
		vpnName = (String) jcsmpSession.getProperty(JCSMPProperties.VPN_NAME);
	}

	@AfterEach
	public void cleanup() {
		if (producer != null) {
			producer.close();
		}

		if (closeErrorQueueInfrastructureCallback != null) {
			closeErrorQueueInfrastructureCallback.run();
		}
	}

	@CartesianTest(name = "[{index}] numMessages={0}, isDurable={1}")
	public void testAccept(@Values(ints = {1, 255}) int numMessages,
						   @Values(booleans = {false, true}) boolean isDurable,
						   JCSMPSession jcsmpSession, Queue durableQueue, SempV2Api sempV2Api)
			throws Exception {
		Queue queue = isDurable ? durableQueue : jcsmpSession.createTemporaryQueue();
		FlowReceiverContainer flowReceiverContainer = initializeFlowReceiverContainer(jcsmpSession, queue);
		JCSMPAcknowledgementCallbackFactory acknowledgementCallbackFactory = new JCSMPAcknowledgementCallbackFactory(
				flowReceiverContainer, !isDurable);
		List<MessageContainer> messageContainers = sendAndReceiveMessages(queue, flowReceiverContainer, numMessages);
		AcknowledgmentCallback acknowledgmentCallback = createAcknowledgmentCallback(acknowledgementCallbackFactory,
				messageContainers);

		acknowledgmentCallback.acknowledge(AcknowledgmentCallback.Status.ACCEPT);
		assertThat(acknowledgmentCallback.isAcknowledged()).isTrue();
		validateNumEnqueuedMessages(sempV2Api, queue.getName(), 0);
		validateNumRedeliveredMessages(sempV2Api, queue.getName(), 0);
		validateQueueBindSuccesses(sempV2Api, queue.getName(), 1);
	}

	@CartesianTest(name = "[{index}] numMessages={0}, isDurable={1}")
	public void testReject(@Values(ints = {1, 255}) int numMessages,
						   @Values(booleans = {false, true}) boolean isDurable,
						   JCSMPSession jcsmpSession,
						   Queue durableQueue,
						   SempV2Api sempV2Api)
			throws Exception {
		Queue queue = isDurable ? durableQueue : jcsmpSession.createTemporaryQueue();
		FlowReceiverContainer flowReceiverContainer = initializeFlowReceiverContainer(jcsmpSession, queue);
		JCSMPAcknowledgementCallbackFactory acknowledgementCallbackFactory = new JCSMPAcknowledgementCallbackFactory(
				flowReceiverContainer, !isDurable);
		List<MessageContainer> messageContainers = sendAndReceiveMessages(queue, flowReceiverContainer, numMessages);
		AcknowledgmentCallback acknowledgmentCallback = createAcknowledgmentCallback(acknowledgementCallbackFactory,
				messageContainers);

		acknowledgmentCallback.acknowledge(AcknowledgmentCallback.Status.REJECT);
		assertThat(acknowledgmentCallback.isAcknowledged()).isTrue();

		//TODO: Remove Commented Code
		/*if (isDurable) {
			// Message was redelivered
			validateNumRedeliveredMessages(sempV2Api, queue.getName(), numMessages);
			validateQueueBindSuccesses(sempV2Api, queue.getName(), 2);
			validateNumEnqueuedMessages(sempV2Api, queue.getName(), numMessages);
		} else */{
			// Message was discarded
			validateNumEnqueuedMessages(sempV2Api, queue.getName(), 0);
			validateNumRedeliveredMessages(sempV2Api, queue.getName(), 0);
			validateQueueBindSuccesses(sempV2Api, queue.getName(), 1);
		}
	}

	@ParameterizedTest(name = "[{index}] numMessages={0}")
	@ValueSource(ints = {1, 255})
	public void testRejectFail(int numMessages,
							   JCSMPSession jcsmpSession,
							   Queue queue,
							   SempV2Api sempV2Api) throws Exception {
		FlowReceiverContainer flowReceiverContainer = initializeFlowReceiverContainer(jcsmpSession, queue);
		JCSMPAcknowledgementCallbackFactory acknowledgementCallbackFactory = new JCSMPAcknowledgementCallbackFactory(
				flowReceiverContainer, !queue.isDurable());
		List<MessageContainer> messageContainers = sendAndReceiveMessages(queue, flowReceiverContainer, numMessages);
		AcknowledgmentCallback acknowledgmentCallback = createAcknowledgmentCallback(acknowledgementCallbackFactory,
				messageContainers);

		logger.info(String.format("Disabling egress for queue %s", queue.getName()));
		sempV2Api.config().updateMsgVpnQueue((String) jcsmpSession.getProperty(JCSMPProperties.VPN_NAME),
				queue.getName(), new ConfigMsgVpnQueue().egressEnabled(false), null);
		retryAssert(() -> assertFalse(sempV2Api.monitor()
				.getMsgVpnQueue(vpnName, queue.getName(), null)
				.getData()
				.isEgressEnabled()));

		logger.info("Acknowledging messages");
		acknowledgmentCallback.acknowledge(AcknowledgmentCallback.Status.REJECT);
		assertThat(acknowledgmentCallback.isAcknowledged()).isTrue();
		Thread.sleep(TimeUnit.SECONDS.toMillis(3));

		logger.info(String.format("Verifying %s message containers haven't been ack'd", numMessages));
		//TODO: Remove Commented Code
		/*assertThat(messageContainers).allSatisfy(messageContainer -> {
			//assertThat(messageContainer.isAcknowledged()).isFalse();
			assertThat(messageContainer.isStale()).isTrue();
		});*/

		validateNumEnqueuedMessages(sempV2Api, queue.getName(), numMessages);
		validateNumRedeliveredMessages(sempV2Api, queue.getName(), 0);
		validateQueueBindSuccesses(sempV2Api, queue.getName(), 1);
		assertThat(sempV2Api.monitor()
				.getMsgVpnQueue(vpnName, queue.getName(), null)
				.getData()
				.getBindRequestCount())
				.isGreaterThan(1);

		logger.info(String.format("Enabling egress for queue %s", queue.getName()));
		sempV2Api.config().updateMsgVpnQueue((String) jcsmpSession.getProperty(JCSMPProperties.VPN_NAME),
				queue.getName(), new ConfigMsgVpnQueue().egressEnabled(true), null);
		retryAssert(() -> assertTrue(sempV2Api.monitor()
				.getMsgVpnQueue(vpnName, queue.getName(), null)
				.getData()
				.isEgressEnabled()));
		Thread.sleep(TimeUnit.SECONDS.toMillis(5)); // rebind task retry interval

		// Message was redelivered
		logger.info("Verifying message was redelivered");
		validateNumRedeliveredMessages(sempV2Api, queue.getName(), numMessages);
		validateQueueBindSuccesses(sempV2Api, queue.getName(), 2);
		validateNumEnqueuedMessages(sempV2Api, queue.getName(), numMessages);
	}

	@CartesianTest(name = "[{index}] numMessages={0}, isDurable={1}")
	public void testRejectWithErrorQueue(@Values(ints = {1, 255}) int numMessages,
										 @Values(booleans = {false, true}) boolean isDurable,
										 JCSMPSession jcsmpSession,
										 Queue durableQueue,
										 SempV2Api sempV2Api) throws Exception {
		Queue queue = isDurable ? durableQueue : jcsmpSession.createTemporaryQueue();
		FlowReceiverContainer flowReceiverContainer = initializeFlowReceiverContainer(jcsmpSession, queue);
		JCSMPAcknowledgementCallbackFactory acknowledgementCallbackFactory = new JCSMPAcknowledgementCallbackFactory(
				flowReceiverContainer, !isDurable);

		List<MessageContainer> messageContainers = sendAndReceiveMessages(queue, flowReceiverContainer, numMessages);
		ErrorQueueInfrastructure errorQueueInfrastructure = initializeErrorQueueInfrastructure(jcsmpSession,
				acknowledgementCallbackFactory);
		AcknowledgmentCallback acknowledgmentCallback = createAcknowledgmentCallback(acknowledgementCallbackFactory,
				messageContainers);

		acknowledgmentCallback.acknowledge(AcknowledgmentCallback.Status.REJECT);

		assertThat(acknowledgmentCallback.isAcknowledged()).isTrue();
		validateNumEnqueuedMessages(sempV2Api, queue.getName(), 0);
		validateNumRedeliveredMessages(sempV2Api, queue.getName(), 0);
		validateQueueBindSuccesses(sempV2Api, queue.getName(), 1);
		validateNumEnqueuedMessages(sempV2Api, errorQueueInfrastructure.getErrorQueueName(), numMessages);
		validateNumRedeliveredMessages(sempV2Api, errorQueueInfrastructure.getErrorQueueName(), 0);
	}

	@CartesianTest(name = "[{index}] numMessages={0}, isDurable={1}")
	public void testRejectWithErrorQueueFail(@Values(ints = {1, 255}) int numMessages,
											 @Values(booleans = {false, true}) boolean isDurable,
											 JCSMPSession jcsmpSession,
											 Queue durableQueue,
											 SempV2Api sempV2Api) throws Exception {
		Queue queue = isDurable ? durableQueue : jcsmpSession.createTemporaryQueue();
		FlowReceiverContainer flowReceiverContainer = initializeFlowReceiverContainer(jcsmpSession, queue);
		JCSMPAcknowledgementCallbackFactory acknowledgementCallbackFactory = new JCSMPAcknowledgementCallbackFactory(
				flowReceiverContainer, !isDurable);

		List<MessageContainer> messageContainers = sendAndReceiveMessages(queue, flowReceiverContainer, numMessages);
		ErrorQueueInfrastructure errorQueueInfrastructure = initializeErrorQueueInfrastructure(jcsmpSession,
				acknowledgementCallbackFactory);
		AcknowledgmentCallback acknowledgmentCallback = createAcknowledgmentCallback(acknowledgementCallbackFactory,
				messageContainers);

		logger.info(String.format("Disabling ingress for error queue %s",
				errorQueueInfrastructure.getErrorQueueName()));
		sempV2Api.config().updateMsgVpnQueue((String) jcsmpSession.getProperty(JCSMPProperties.VPN_NAME),
				errorQueueInfrastructure.getErrorQueueName(), new ConfigMsgVpnQueue().ingressEnabled(false), null);
		retryAssert(() -> assertFalse(sempV2Api.monitor()
				.getMsgVpnQueue(vpnName, errorQueueInfrastructure.getErrorQueueName(), null)
				.getData()
				.isIngressEnabled()));

		logger.info("Acknowledging messages");
		acknowledgmentCallback.acknowledge(AcknowledgmentCallback.Status.REJECT);
		assertThat(acknowledgmentCallback.isAcknowledged()).isTrue();

		validateNumRedeliveredMessages(sempV2Api, queue.getName(), numMessages);
		//validateQueueBindSuccesses(sempV2Api, queue.getName(), 2);
		validateNumEnqueuedMessages(sempV2Api, queue.getName(), numMessages);

		//TODO: Remove Commented Code
		/*if (isDurable) {
			// Message was redelivered
			logger.info("Verifying message was redelivered");
			validateNumRedeliveredMessages(sempV2Api, queue.getName(), numMessages);
			validateQueueBindSuccesses(sempV2Api, queue.getName(), 2);
			validateNumEnqueuedMessages(sempV2Api, queue.getName(), numMessages);
		} else {
			// Message was discarded
			logger.info("Verifying message was discarded");
			validateNumEnqueuedMessages(sempV2Api, queue.getName(), 0);
			validateNumRedeliveredMessages(sempV2Api, queue.getName(), 0);
			validateQueueBindSuccesses(sempV2Api, queue.getName(), 1);
		}*/

		validateNumEnqueuedMessages(sempV2Api, errorQueueInfrastructure.getErrorQueueName(), 0);
		validateNumRedeliveredMessages(sempV2Api, errorQueueInfrastructure.getErrorQueueName(), 0);
	}

	@CartesianTest(name = "[{index}] numMessages={0}, isDurable={1}")
	public void testRequeue(@Values(ints = {1, 255}) int numMessages,
							@Values(booleans = {false, true}) boolean isDurable,
							JCSMPSession jcsmpSession,
							Queue durableQueue,
							SempV2Api sempV2Api) throws Exception {
		Queue queue = isDurable ? durableQueue : jcsmpSession.createTemporaryQueue();
		FlowReceiverContainer flowReceiverContainer = initializeFlowReceiverContainer(jcsmpSession, queue);
		JCSMPAcknowledgementCallbackFactory acknowledgementCallbackFactory = new JCSMPAcknowledgementCallbackFactory(
				flowReceiverContainer, !isDurable);

		List<MessageContainer> messageContainers = sendAndReceiveMessages(queue, flowReceiverContainer, numMessages);
		AcknowledgmentCallback acknowledgmentCallback = createAcknowledgmentCallback(acknowledgementCallbackFactory,
				messageContainers);

		//TODO: Remove Commented Code
		//if (isDurable) {
			acknowledgmentCallback.acknowledge(AcknowledgmentCallback.Status.REQUEUE);
			assertThat(acknowledgmentCallback.isAcknowledged()).isTrue();
			validateNumRedeliveredMessages(sempV2Api, queue.getName(), numMessages);
			validateQueueBindSuccesses(sempV2Api, queue.getName(), 1);
			validateNumEnqueuedMessages(sempV2Api, queue.getName(), numMessages);
		/*} else {
			SolaceAcknowledgmentException thrown = assertThrows(SolaceAcknowledgmentException.class,
					() -> acknowledgmentCallback.acknowledge(AcknowledgmentCallback.Status.REQUEUE));
			assertThat(acknowledgmentCallback.isAcknowledged()).isFalse();
			assertThat(thrown).hasRootCauseInstanceOf(UnsupportedOperationException.class);
			assertThat(ExceptionUtils.getRootCause(thrown.getCause()))
					.hasMessageContaining("not supported with temporary queues");
		}*/
	}

	@ParameterizedTest(name = "[{index}] numMessages={0}")
	@ValueSource(ints = {1, 255})
	public void testRequeueFail(int numMessages, JCSMPSession jcsmpSession, Queue queue, SempV2Api sempV2Api)
			throws Exception {
		FlowReceiverContainer flowReceiverContainer = initializeFlowReceiverContainer(jcsmpSession, queue);
		JCSMPAcknowledgementCallbackFactory acknowledgementCallbackFactory = new JCSMPAcknowledgementCallbackFactory(
				flowReceiverContainer, false);

		List<MessageContainer> messageContainers = sendAndReceiveMessages(queue, flowReceiverContainer, numMessages);
		AcknowledgmentCallback acknowledgmentCallback = createAcknowledgmentCallback(acknowledgementCallbackFactory,
				messageContainers);

		logger.info(String.format("Disabling egress for queue %s", queue.getName()));
		sempV2Api.config().updateMsgVpnQueue((String) jcsmpSession.getProperty(JCSMPProperties.VPN_NAME),
				queue.getName(), new ConfigMsgVpnQueue().egressEnabled(false), null);
		retryAssert(() -> assertFalse(sempV2Api.monitor()
				.getMsgVpnQueue(vpnName, queue.getName(), null)
				.getData()
				.isEgressEnabled()));

		logger.info("Acknowledging messages");
		acknowledgmentCallback.acknowledge(AcknowledgmentCallback.Status.REQUEUE);
		assertThat(acknowledgmentCallback.isAcknowledged()).isTrue();
		Thread.sleep(TimeUnit.SECONDS.toMillis(3));

		logger.info(String.format("Verifying %s message containers haven't been ack'd", numMessages));
		//TODO: Remove Commented Code
		/*assertThat(messageContainers).allSatisfy(messageContainer -> {
			assertThat(messageContainer.isAcknowledged()).isFalse();
			assertThat(messageContainer.isStale()).isTrue();
		});*/
		validateNumEnqueuedMessages(sempV2Api, queue.getName(), numMessages);
		validateNumRedeliveredMessages(sempV2Api, queue.getName(), 0);
		validateQueueBindSuccesses(sempV2Api, queue.getName(), 1);
		assertThat(sempV2Api.monitor()
				.getMsgVpnQueue(vpnName, queue.getName(), null)
				.getData()
				.getBindRequestCount())
				.isGreaterThan(1);

		logger.info(String.format("Enabling egress for queue %s", queue.getName()));
		sempV2Api.config().updateMsgVpnQueue((String) jcsmpSession.getProperty(JCSMPProperties.VPN_NAME),
				queue.getName(), new ConfigMsgVpnQueue().egressEnabled(true), null);
		retryAssert(() -> assertTrue(sempV2Api.monitor()
				.getMsgVpnQueue(vpnName, queue.getName(), null)
				.getData()
				.isEgressEnabled()));
		Thread.sleep(TimeUnit.SECONDS.toMillis(5)); // rebind task retry interval

		// Message was redelivered
		logger.info("Verifying message was redelivered");
		validateNumRedeliveredMessages(sempV2Api, queue.getName(), numMessages);
		validateQueueBindSuccesses(sempV2Api, queue.getName(), 2);
		validateNumEnqueuedMessages(sempV2Api, queue.getName(), numMessages);
	}

	//TODO: Remove Commented Code
	/*@ParameterizedTest(name = "[{index}] numMessages={0}")
	@ValueSource(ints = {1, 255})
	public void testRequeueDelegateWhileRebinding(int numMessages,
												  JCSMPSession jcsmpSession,
												  Queue queue,
												  SempV2Api sempV2Api,
												  @ExecSvc(poolSize = 1) ExecutorService executorService)
			throws Exception {
		FlowReceiverContainer flowReceiverContainer = initializeFlowReceiverContainer(jcsmpSession, queue);
		JCSMPAcknowledgementCallbackFactory acknowledgementCallbackFactory = new JCSMPAcknowledgementCallbackFactory(
				flowReceiverContainer, false);

		List<MessageContainer> messageContainers = sendAndReceiveMessages(queue, flowReceiverContainer, numMessages);
		MessageContainer blockingContainer = sendAndReceiveMessages(queue, flowReceiverContainer, 1).get(0);
		AcknowledgmentCallback acknowledgmentCallback = createAcknowledgmentCallback(acknowledgementCallbackFactory,
				messageContainers);

		Future<UUID> rebindFuture = executorService.submit(() ->
				flowReceiverContainer.rebind(messageContainers.get(0).getFlowReceiverReferenceId()));
		Thread.sleep(1000);
		assertFalse(rebindFuture.isDone());

		for (MessageContainer messageContainer : messageContainers) {
			// force async rebind
			Mockito.doReturn(null).doCallRealMethod().when(flowReceiverContainer)
					.acknowledgeRebind(messageContainer, true);
		}

		logger.info(String.format("Acknowledging %s message containers", messageContainers.size()));
		acknowledgmentCallback.acknowledge(AcknowledgmentCallback.Status.REQUEUE);
		assertThat(acknowledgmentCallback.isAcknowledged()).isTrue();

		*//*if (acknowledgmentCallback instanceof JCSMPBatchAcknowledgementCallback) {
			Mockito.verify(retryableTaskService, Mockito.times(2)).submit(
					new RetryableRebindTask(flowReceiverContainer,
							messageContainers.get(0).getFlowReceiverReferenceId(), retryableTaskService));
		} else {
			for (MessageContainer messageContainer : messageContainers) {
				Mockito.verify(retryableTaskService).submit(
						new RetryableAckRebindTask(flowReceiverContainer, messageContainer, retryableTaskService));
			}
		}*//*

		Thread.sleep(TimeUnit.SECONDS.toMillis(3));

		logger.info(String.format("Verifying %s message containers haven't been ack'd", messageContainers.size()));
		assertThat(messageContainers).allSatisfy(messageContainer -> {
			assertThat(messageContainer.isAcknowledged())
					.isEqualTo(acknowledgmentCallback instanceof JCSMPAcknowledgementCallback ||
							messageContainer.equals(messageContainers.get(messageContainers.size() - 1)));
			assertThat(messageContainer.isStale()).isFalse();
		});
		assertFalse(rebindFuture.isDone());
		validateNumEnqueuedMessages(sempV2Api, queue.getName(), numMessages + 1);
		validateNumRedeliveredMessages(sempV2Api, queue.getName(), 0);
		validateQueueBindSuccesses(sempV2Api, queue.getName(), 1);

		logger.info("Acknowledging blocking message");
		flowReceiverContainer.acknowledge(blockingContainer);
		Thread.sleep(TimeUnit.SECONDS.toMillis(5)); // rebind task retry interval
		assertNotNull(rebindFuture.get(1, TimeUnit.MINUTES));

		// Message was redelivered
		logger.info("Verifying message was redelivered");
		validateNumRedeliveredMessages(sempV2Api, queue.getName(), numMessages);
		validateQueueBindSuccesses(sempV2Api, queue.getName(), 2);
		validateNumEnqueuedMessages(sempV2Api, queue.getName(), numMessages);
	}*/

	@CartesianTest(name = "[{index}] numMessages={0}, isDurable={1}")
	public void testReAckAfterAccept(@Values(ints = {1, 255}) int numMessages,
									 @Values(booleans = {false, true}) boolean isDurable,
									 JCSMPSession jcsmpSession,
									 Queue durableQueue,
									 SempV2Api sempV2Api) throws Throwable {
		Queue queue = isDurable ? durableQueue : jcsmpSession.createTemporaryQueue();
		FlowReceiverContainer flowReceiverContainer = initializeFlowReceiverContainer(jcsmpSession, queue);
		JCSMPAcknowledgementCallbackFactory acknowledgementCallbackFactory = new JCSMPAcknowledgementCallbackFactory(
				flowReceiverContainer, !isDurable);

		List<MessageContainer> messageContainers = sendAndReceiveMessages(queue, flowReceiverContainer, numMessages);
		AcknowledgmentCallback acknowledgmentCallback = createAcknowledgmentCallback(acknowledgementCallbackFactory,
				messageContainers);

		ThrowingRunnable verifyExpectedState = () -> {
			validateNumEnqueuedMessages(sempV2Api, queue.getName(), 0);
			validateNumRedeliveredMessages(sempV2Api, queue.getName(), 0);
			validateQueueBindSuccesses(sempV2Api, queue.getName(), 1);
		};

		acknowledgmentCallback.acknowledge(AcknowledgmentCallback.Status.ACCEPT);
		assertThat(acknowledgmentCallback.isAcknowledged()).isTrue();
		verifyExpectedState.run();

		for (AcknowledgmentCallback.Status status : AcknowledgmentCallback.Status.values()) {
			acknowledgmentCallback.acknowledge(status);
			assertThat(acknowledgmentCallback.isAcknowledged()).isTrue();
			verifyExpectedState.run();
		}
	}

	@CartesianTest(name = "[{index}] numMessages={0}, isDurable={1}")
	public void testReAckAfterReject(@Values(ints = {1, 255}) int numMessages,
									 @Values(booleans = {false, true}) boolean isDurable,
									 JCSMPSession jcsmpSession,
									 Queue durableQueue,
									 SempV2Api sempV2Api) throws Throwable {
		Queue queue = isDurable ? durableQueue : jcsmpSession.createTemporaryQueue();
		FlowReceiverContainer flowReceiverContainer = initializeFlowReceiverContainer(jcsmpSession, queue);
		JCSMPAcknowledgementCallbackFactory acknowledgementCallbackFactory = new JCSMPAcknowledgementCallbackFactory(
				flowReceiverContainer, !isDurable);

		List<MessageContainer> messageContainers = sendAndReceiveMessages(queue, flowReceiverContainer, numMessages);
		AcknowledgmentCallback acknowledgmentCallback = createAcknowledgmentCallback(acknowledgementCallbackFactory,
				messageContainers);

		//TODO: Remove Commented Code
		ThrowingRunnable verifyExpectedState = () -> {
			/*if (isDurable) {
				// Message was redelivered
				validateNumRedeliveredMessages(sempV2Api, queue.getName(), numMessages);
				validateQueueBindSuccesses(sempV2Api, queue.getName(), 2);
				validateNumEnqueuedMessages(sempV2Api, queue.getName(), numMessages);
			} else */{
				// Message was discarded
				validateNumEnqueuedMessages(sempV2Api, queue.getName(), 0);
				validateNumRedeliveredMessages(sempV2Api, queue.getName(), 0);
				validateQueueBindSuccesses(sempV2Api, queue.getName(), 1);
			}
		};

		acknowledgmentCallback.acknowledge(AcknowledgmentCallback.Status.REJECT);
		assertThat(acknowledgmentCallback.isAcknowledged()).isTrue();
		verifyExpectedState.run();

		for (AcknowledgmentCallback.Status status : AcknowledgmentCallback.Status.values()) {
			acknowledgmentCallback.acknowledge(status);
			assertThat(acknowledgmentCallback.isAcknowledged()).isTrue();
			verifyExpectedState.run();
		}
	}

	@CartesianTest(name = "[{index}] numMessages={0}, isDurable={1}")
	public void testReAckAfterRejectWithErrorQueue(@Values(ints = {1, 255}) int numMessages,
												   @Values(booleans = {false, true}) boolean isDurable,
												   JCSMPSession jcsmpSession,
												   Queue durableQueue,
												   SempV2Api sempV2Api) throws Throwable {
		Queue queue = isDurable ? durableQueue : jcsmpSession.createTemporaryQueue();
		FlowReceiverContainer flowReceiverContainer = initializeFlowReceiverContainer(jcsmpSession, queue);
		JCSMPAcknowledgementCallbackFactory acknowledgementCallbackFactory = new JCSMPAcknowledgementCallbackFactory(
				flowReceiverContainer, !isDurable);

		List<MessageContainer> messageContainers = sendAndReceiveMessages(queue, flowReceiverContainer, numMessages);
		ErrorQueueInfrastructure errorQueueInfrastructure = initializeErrorQueueInfrastructure(jcsmpSession,
				acknowledgementCallbackFactory);
		AcknowledgmentCallback acknowledgmentCallback = createAcknowledgmentCallback(acknowledgementCallbackFactory,
				messageContainers);

		ThrowingRunnable verifyExpectedState = () -> {
			validateNumEnqueuedMessages(sempV2Api, queue.getName(), 0);
			validateNumRedeliveredMessages(sempV2Api, queue.getName(), 0);
			validateQueueBindSuccesses(sempV2Api, queue.getName(), 1);
			validateNumEnqueuedMessages(sempV2Api, errorQueueInfrastructure.getErrorQueueName(), numMessages);
			validateNumRedeliveredMessages(sempV2Api, errorQueueInfrastructure.getErrorQueueName(), 0);
		};

		acknowledgmentCallback.acknowledge(AcknowledgmentCallback.Status.REJECT);
		assertThat(acknowledgmentCallback.isAcknowledged()).isTrue();
		verifyExpectedState.run();

		for (AcknowledgmentCallback.Status status : AcknowledgmentCallback.Status.values()) {
			acknowledgmentCallback.acknowledge(status);
			assertThat(acknowledgmentCallback.isAcknowledged()).isTrue();
			verifyExpectedState.run();
		}
	}

	@ParameterizedTest(name = "[{index}] numMessages={0}")
	@ValueSource(ints = {1, 255})
	public void testReAckAfterRequeue(int numMessages,
									  JCSMPSession jcsmpSession,
									  Queue queue,
									  SempV2Api sempV2Api) throws Throwable {
		FlowReceiverContainer flowReceiverContainer = initializeFlowReceiverContainer(jcsmpSession, queue);
		JCSMPAcknowledgementCallbackFactory acknowledgementCallbackFactory = new JCSMPAcknowledgementCallbackFactory(
				flowReceiverContainer, false);

		List<MessageContainer> messageContainers = sendAndReceiveMessages(queue, flowReceiverContainer, numMessages);
		AcknowledgmentCallback acknowledgmentCallback = createAcknowledgmentCallback(acknowledgementCallbackFactory,
				messageContainers);

		//TODO: Remove Commented Code
		ThrowingRunnable verifyExpectedState = () -> {
			validateNumRedeliveredMessages(sempV2Api, queue.getName(), numMessages);
			//validateQueueBindSuccesses(sempV2Api, queue.getName(), 2);
			validateNumEnqueuedMessages(sempV2Api, queue.getName(), numMessages);
		};

		acknowledgmentCallback.acknowledge(AcknowledgmentCallback.Status.REQUEUE);
		assertThat(acknowledgmentCallback.isAcknowledged()).isTrue();
		verifyExpectedState.run();

		for (AcknowledgmentCallback.Status status : AcknowledgmentCallback.Status.values()) {
			acknowledgmentCallback.acknowledge(status);
			assertThat(acknowledgmentCallback.isAcknowledged()).isTrue();
			verifyExpectedState.run();
		}
	}

	@CartesianTest(name = "[{index}] numMessages={0}, isDurable={1}, createErrorQueue={2}")
	public void testAckStaleMessage(@Values(ints = {1, 255}) int numMessages,
									@Values(booleans = {false, true}) boolean isDurable,
									@Values(booleans = {false, true}) boolean createErrorQueue,
									JCSMPSession jcsmpSession,
									Queue durableQueue,
									SempV2Api sempV2Api) throws Exception {
		Queue queue = isDurable ? durableQueue : jcsmpSession.createTemporaryQueue();
		FlowReceiverContainer flowReceiverContainer = initializeFlowReceiverContainer(jcsmpSession, queue);
		JCSMPAcknowledgementCallbackFactory acknowledgementCallbackFactory = new JCSMPAcknowledgementCallbackFactory(
				flowReceiverContainer, !isDurable);

		List<MessageContainer> messageContainers = sendAndReceiveMessages(queue, flowReceiverContainer, numMessages)
				.stream()
				.map(Mockito::spy)
				.peek(m -> Mockito.when(m.isStale()).thenReturn(true))
				.collect(Collectors.toList());
		Optional<String> errorQueueName = Optional.of(createErrorQueue)
				.filter(e -> e)
				.map(c -> initializeErrorQueueInfrastructure(jcsmpSession, acknowledgementCallbackFactory))
				.map(ErrorQueueInfrastructure::getErrorQueueName);
		AcknowledgmentCallback acknowledgmentCallback = createAcknowledgmentCallback(acknowledgementCallbackFactory,
				messageContainers);

		for (AcknowledgmentCallback.Status status : AcknowledgmentCallback.Status.values()) {
			SolaceAcknowledgmentException exception = assertThrows(SolaceAcknowledgmentException.class,
					() -> acknowledgmentCallback.acknowledge(status));
			//TODO: Remove Commented Code
			Class<? extends Throwable> expectedRootCause = /*!isDurable &&
					status.equals(AcknowledgmentCallback.Status.REQUEUE) ? UnsupportedOperationException.class :*/
					SolaceStaleMessageException.class;

			assertThat(acknowledgmentCallback.isAcknowledged())
					.describedAs("Unexpected ack state for %s re-ack", status)
					.isFalse();
			assertThat(exception)
					.describedAs("Unexpected root cause for %s re-ack", status)
					.hasRootCauseInstanceOf(expectedRootCause);
			if (exception instanceof SolaceBatchAcknowledgementException) {
				assertThat(((SolaceBatchAcknowledgementException) exception).isAllStaleExceptions())
						.describedAs("Unexpected stale batch state for %s re-ack", status)
						.isEqualTo(expectedRootCause.equals(SolaceStaleMessageException.class));
			}
			validateNumEnqueuedMessages(sempV2Api, queue.getName(), numMessages);
			validateNumRedeliveredMessages(sempV2Api, queue.getName(), 0);
			validateQueueBindSuccesses(sempV2Api, queue.getName(), 1);
			if (errorQueueName.isPresent()) {
				validateNumEnqueuedMessages(sempV2Api, errorQueueName.get(), 0);
				validateNumRedeliveredMessages(sempV2Api, errorQueueName.get(), 0);
			}
		}
	}

	private List<MessageContainer> sendAndReceiveMessages(Queue queue,
														  FlowReceiverContainer flowReceiverContainer,
														  int numMessages)
			throws JCSMPException, UnboundFlowReceiverContainerException {
		assertThat(numMessages).isGreaterThan(0);

		for (int i = 0; i < numMessages; i++) {
			producer.send(JCSMPFactory.onlyInstance().createMessage(TextMessage.class), queue);
		}

		if (numMessages > 1) {
			List<MessageContainer> messageContainers = new ArrayList<>();
			for (int i = 0; i < numMessages; i++) {
				MessageContainer messageContainer = flowReceiverContainer.receive((int)
						TimeUnit.SECONDS.toMillis(10));
				assertNotNull(messageContainer);
				messageContainers.add(messageContainer);
			}
			return messageContainers;
		} else {
			MessageContainer messageContainer = flowReceiverContainer.receive((int)
					TimeUnit.SECONDS.toMillis(10));
			assertNotNull(messageContainer);
			return Collections.singletonList(messageContainer);
		}
	}

	private AcknowledgmentCallback createAcknowledgmentCallback(
			JCSMPAcknowledgementCallbackFactory acknowledgementCallbackFactory,
			List<MessageContainer> messageContainers) {
		assertThat(messageContainers).hasSizeGreaterThan(0);
		if (messageContainers.size() > 1) {
			return acknowledgementCallbackFactory.createBatchCallback(messageContainers);
		} else {
			return acknowledgementCallbackFactory.createCallback(messageContainers.get(0));
		}
	}

	private FlowReceiverContainer initializeFlowReceiverContainer(JCSMPSession jcsmpSession, Queue queue)
			throws JCSMPException {
		if (flowReceiverContainerReference.compareAndSet(null, Mockito.spy(new FlowReceiverContainer(
				jcsmpSession,
				queue.getName(),
				new EndpointProperties())))) {
			flowReceiverContainerReference.get().bind();
		}
		return flowReceiverContainerReference.get();
	}

	private ErrorQueueInfrastructure initializeErrorQueueInfrastructure(JCSMPSession jcsmpSession,
															JCSMPAcknowledgementCallbackFactory ackCallbackFactory) {
		if (closeErrorQueueInfrastructureCallback != null) {
			throw new IllegalStateException("Should only have one error queue infrastructure");
		}

		String producerManagerKey = UUID.randomUUID().toString();
		JCSMPSessionProducerManager jcsmpSessionProducerManager = new JCSMPSessionProducerManager(jcsmpSession);
		ErrorQueueInfrastructure errorQueueInfrastructure = new ErrorQueueInfrastructure(jcsmpSessionProducerManager,
				producerManagerKey, RandomStringUtils.randomAlphanumeric(20), new SolaceConsumerProperties());
		Queue errorQueue = JCSMPFactory.onlyInstance().createQueue(errorQueueInfrastructure.getErrorQueueName());
		ackCallbackFactory.setErrorQueueInfrastructure(errorQueueInfrastructure);
		closeErrorQueueInfrastructureCallback = () -> {
			jcsmpSessionProducerManager.release(producerManagerKey);

			try {
				jcsmpSession.deprovision(errorQueue, JCSMPSession.WAIT_FOR_CONFIRM);
			} catch (JCSMPException e) {
				throw new RuntimeException(e);
			}
		};

		try {
			jcsmpSession.provision(errorQueue, new EndpointProperties(), JCSMPSession.WAIT_FOR_CONFIRM);
		} catch (JCSMPException e) {
			throw new RuntimeException(e);
		}

		return errorQueueInfrastructure;
	}

	private void validateNumEnqueuedMessages(SempV2Api sempV2Api, String queueName, int expectedCount)
			throws InterruptedException {
		retryAssert(() -> {
			List<MonitorMsgVpnQueueMsg> messages = new ArrayList<>();
			Optional<String> cursor = Optional.empty();
			do {
				MonitorMsgVpnQueueMsgsResponse response = sempV2Api.monitor()
						.getMsgVpnQueueMsgs(vpnName, queueName, Integer.MAX_VALUE, cursor.orElse(null),
								null, null);
				cursor = Optional.ofNullable(response.getMeta())
						.map(MonitorSempMeta::getPaging)
						.map(MonitorSempPaging::getCursorQuery);
				messages.addAll(response.getData());
			} while (cursor.isPresent());
			assertThat(messages).hasSize(expectedCount);
		});
	}

	private void validateNumRedeliveredMessages(SempV2Api sempV2Api, String queueName, int expectedCount)
			throws InterruptedException {
		retryAssert(() -> assertThat(sempV2Api.monitor()
				.getMsgVpnQueue(vpnName, queueName, null)
				.getData()
				.getRedeliveredMsgCount())
				.isEqualTo(expectedCount));
	}

	private void validateQueueBindSuccesses(SempV2Api sempV2Api, String queueName, int expectedCount)
			throws InterruptedException {
		retryAssert(() -> assertThat(sempV2Api.monitor()
				.getMsgVpnQueue(vpnName, queueName, null)
				.getData()
				.getBindSuccessCount())
				.isEqualTo(expectedCount));
	}
}
