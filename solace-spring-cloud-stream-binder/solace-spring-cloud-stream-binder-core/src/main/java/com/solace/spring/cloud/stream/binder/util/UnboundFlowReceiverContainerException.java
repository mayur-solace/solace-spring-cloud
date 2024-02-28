package com.solace.spring.cloud.stream.binder.util;

//TODO: MP: Fix javadocs
//Review if this exception is still required?
/**
 * <p>Flow receiver container is not bound to a flow receiver.</p>
 * <p>Typically caused by one of:</p>
 * <ul>
 * <li>{@link FlowReceiverContainer#unbind()}</li>
 * <li>An error during {link FlowReceiverContainer#rebind(UUID)}</li>
 * <li>An error during {link FlowReceiverContainer#acknowledgeRebind(MessageContainer)}</li>
 * </ul>
 */
public class UnboundFlowReceiverContainerException extends Exception {
	public UnboundFlowReceiverContainerException(String message) {
		super(message);
	}
}
