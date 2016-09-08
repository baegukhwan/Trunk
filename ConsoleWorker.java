package com.sycros.SAManager;

import java.io.IOException;
import java.lang.reflect.Type;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.sycros.SAManager.DB.SAManagerDao;
import com.sycros.dna.Session2;
import com.sycros.dna.Session2Controller;
import com.sycros.dna.framework.constant.TaskTypeEnum;
import com.sycros.dna.framework.exception.AppException;
import com.sycros.dna.framework.util.AppUtil;
import com.sycros.domain.JobCommand;
import com.sycros.domain.JobRunCommand;
import com.sycros.domain.Result;
import com.sycros.domain.Task;
import com.sycros.domain.vo.SAManagerVo;

public class ConsoleWorker implements Runnable {
	protected Logger logger = LoggerFactory.getLogger(getClass());
	private BlockingQueue<JobCommand> commandQueue;
	private BlockingQueue<List<JobRunCommand>> commandJobRunQueue;
	private SAManagerVo saManagerVo;
	private Session2Controller controller;
	private Gson gson = new Gson();

	public ConsoleWorker(Session2 session, BlockingQueue<JobCommand> commandQueue, BlockingQueue<List<JobRunCommand>> commandJobRunQueue) throws SQLException, AppException, IOException {
		init();
		this.commandQueue = commandQueue;
		this.commandJobRunQueue = commandJobRunQueue;

		controller = new Session2Controller(session, saManagerVo.getConnectionTimeout(), saManagerVo.getBlockDetection(), logger);
		controller.open();
	}

	private void init() throws UnknownHostException, SocketException, SQLException, AppException {
		initDB();
	}

	private void initDB() throws UnknownHostException, SocketException, SQLException, AppException {
		saManagerVo = new SAManagerDao().getManagerInfo();
		if (null == saManagerVo.getPort()) {
			throw new AppException("", "Can't get SA manager information from database.");
		}
	}

	@Override
	public void run() {
		try {
			dispatch();
		} catch (InterruptedException x) {
			logger.debug("run() : " + AppUtil.nullToMessage(x.getMessage()));
		} catch (IOException x) {
			logger.error(AppUtil.getStackTrace2String(x));
		} finally {
			controller.close();
		}
	}

	private void dispatch() throws InterruptedException, IOException {
		while (controller.getSession().isOpen() == true) {
			Object content = controller.getReadQueue().poll(saManagerVo.getConnectionTimeout(), TimeUnit.MILLISECONDS);
			if (content != null && content instanceof Task) {
				Task task = (Task) content;
				if (task.getTaskType().equals(TaskTypeEnum.JobCommand.getType())) {
					processJobCommand(task);
				} else if (task.getTaskType().equals(TaskTypeEnum.JobRunCommand.getType())) {
					processJobRunCommand(task);
				} else {
					processTask(task);
				}
			}
		}
	}

	private void processTask(Task task) throws IOException {
		new ConsoleTask(task, controller.getWriteQueue()).run();
	}

	private void processJobCommand(Task task) throws InterruptedException {
		Type type = new TypeToken<ArrayList<JobCommand>>(){}.getType();
		List<JobCommand> list = gson.fromJson(task.getRequest(), type);
		for (JobCommand jobCommand : list) {
			commandQueue.put(jobCommand);
		}
		controller.getWriteQueue().put(new Result(AppUtil.getLocation(), task));
	}

	private void processJobRunCommand(Task task) throws InterruptedException {
		Type type = new TypeToken<ArrayList<JobRunCommand>>(){}.getType();
		List<JobRunCommand> list = gson.fromJson(task.getRequest(), type);
		commandJobRunQueue.put(list);
	}
}
