package it.polimi.spark;

import java.io.Serializable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

public class Config implements Serializable {

	/**
	 * generated serial version UID
	 */
	private static final long serialVersionUID = 5087417577620830639L;

	private static Config _instance;

	static final Logger logger = LoggerFactory.getLogger(Config.class);

	private Config() {
	}

	public static Config getInstance() {
		if (_instance == null) {
			_instance = new Config();
		}
		return _instance;
	}

	public static void init(String[] args) {
		_instance = new Config();
		new JCommander(_instance, args);
		_instance.outputPathBase += "/" + (new java.util.Date().getTime());
		logger.debug(
				"Configuration: --firstFile {} --secondFile {} --outputPathBase {}  --runlocal {} --waitTime {}",
				new Object[] { _instance.firstFile, _instance.secondFile,
						_instance.outputPathBase, _instance.runLocal, _instance.waitTime });
	}

	@Parameter(names = { "-f", "--firstFile" }, required = true)
	public String firstFile;

	@Parameter(names = { "-s", "--secondFile" }, required = true)
	public String secondFile;

	@Parameter(names = { "-l", "--runLocal" })
	public boolean runLocal;
	
	@Parameter(names = { "-w", "--waitTime" })
	public int waitTime = 0;


	@Parameter(names = { "-o", "--outputPathBase" }, required = true)
	public String outputPathBase;

}
