package com.xiaoseller.multi.transaction;

import java.util.Arrays;
import java.util.Stack;

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.DefaultTransactionDefinition;

@Aspect
public class MultiTransactionalAspect implements ApplicationContextAware {
	private ApplicationContext applicationContext;
	private Logger logger = LoggerFactory.getLogger(MultiTransactionalAspect.class);

	@Around("@annotation(multiTransactional)")
	public Object around(ProceedingJoinPoint pjp, MultiTransactional multiTransactional) throws Throwable {
		Stack<DataSourceTransactionManager> dataSourceTransactionManagerStack = new Stack<DataSourceTransactionManager>();
		Stack<TransactionStatus> transactionStatuStack = new Stack<TransactionStatus>();

		try {
			if (!this.openTransaction(dataSourceTransactionManagerStack, transactionStatuStack, multiTransactional)) {
				return null;
			} else {
				Object e = pjp.proceed();
				this.commit(dataSourceTransactionManagerStack, transactionStatuStack);
				return e;
			}
		} catch (Throwable arg5) {
			this.rollback(dataSourceTransactionManagerStack, transactionStatuStack);
			this.logger.warn(
					String.format("MultiTransactionalAspect, method:%s-%s occors error:",
							new Object[] { pjp.getTarget().getClass().getSimpleName(), pjp.getSignature().getName() }),
					arg5);
			throw arg5;
		}
	}

	private boolean openTransaction(Stack<DataSourceTransactionManager> dataSourceTransactionManagerStack,
			Stack<TransactionStatus> transactionStatuStack, MultiTransactional multiTransactional) {
		String[] transactionMangerNames = multiTransactional.values();
		this.logger.debug("openTransaction:{}", Arrays.asList(transactionMangerNames));
		if (transactionMangerNames != null && transactionMangerNames.length != 0) {
			String[] arg4 = transactionMangerNames;
			int arg5 = transactionMangerNames.length;

			for (int arg6 = 0; arg6 < arg5; ++arg6) {
				String beanName = arg4[arg6];
				DataSourceTransactionManager dataSourceTransactionManager = (DataSourceTransactionManager) this.applicationContext
						.getBean(beanName, DataSourceTransactionManager.class);
				TransactionStatus transactionStatus = dataSourceTransactionManager
						.getTransaction(new DefaultTransactionDefinition());
				transactionStatuStack.push(transactionStatus);
				dataSourceTransactionManagerStack.push(dataSourceTransactionManager);
			}

			return true;
		} else {
			return false;
		}
	}

	private void commit(Stack<DataSourceTransactionManager> dataSourceTransactionManagerStack,
			Stack<TransactionStatus> transactionStatuStack) {
		while (!dataSourceTransactionManagerStack.isEmpty()) {
			((DataSourceTransactionManager) dataSourceTransactionManagerStack.pop())
					.commit((TransactionStatus) transactionStatuStack.pop());
		}

		this.logger.debug("commit");
	}

	private void rollback(Stack<DataSourceTransactionManager> dataSourceTransactionManagerStack,
			Stack<TransactionStatus> transactionStatuStack) {
		while (!dataSourceTransactionManagerStack.isEmpty()) {
			((DataSourceTransactionManager) dataSourceTransactionManagerStack.pop())
					.rollback((TransactionStatus) transactionStatuStack.pop());
		}

	}

	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.applicationContext = applicationContext;
	}
}
