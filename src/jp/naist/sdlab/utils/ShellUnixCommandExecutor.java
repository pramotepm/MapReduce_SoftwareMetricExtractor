package jp.naist.sdlab.utils;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.util.Shell;

public class ShellUnixCommandExecutor extends Shell {
	private String[] command;
	private StringBuffer output;
	private String stdinput;
	private int exitCode;
	
	public ShellUnixCommandExecutor(List<String> command) {
		// TODO Auto-generated constructor stub
		this.command = command.toArray(new String[command.size()]);
		this.stdinput = null;
	}

	public ShellUnixCommandExecutor(List<String> command, String stdinput) {
		// TODO Auto-generated constructor stub
		this.command = command.toArray(new String[command.size()]);
		this.stdinput = stdinput;
	}

	public ShellUnixCommandExecutor(String[] command) {
		this.command = command;
		this.stdinput = null;
	}
	
	public ShellUnixCommandExecutor(String[] command, String stdinput) {
		this.command = command;
		this.stdinput = stdinput;		
	}
	
	public void execute() throws IOException {
		this.run();
	}
	
	@Override
	public int getExitCode() {
		return this.exitCode;
	}

	@Override
	protected void run() throws IOException {
		ProcessBuilder pb = new ProcessBuilder(this.command);
		Process proc = pb.start();
		AtomicBoolean completed = new AtomicBoolean(false);
		
		final BufferedReader errReader = new BufferedReader(new InputStreamReader(proc.getErrorStream()));
		BufferedReader inReader = new BufferedReader(new InputStreamReader(proc.getInputStream()));
		
		if (this.stdinput != null) {
			// Pass input into processs's input stream
			BufferedWriter outWriter = new BufferedWriter(new OutputStreamWriter(proc.getOutputStream()));
			outWriter.write(this.stdinput);
			outWriter.flush();
			outWriter.close();
		}
		
		final StringBuffer errMsg = new StringBuffer();
	    
	    Thread errThread = new Thread() {
	    	@Override
	    	public void run() {
	    		try {
	    			String line = errReader.readLine();
	    			while((line != null) && !isInterrupted()) {
	    				errMsg.append(line);
	    				errMsg.append(System.getProperty("line.separator"));
	    				line = errReader.readLine();
	    			}
	    		} 
	    		catch(IOException ioe) { }
	    	}
	    };
	    try {
	    	errThread.start();
	    } catch (IllegalStateException ise) {
	    	
	    }
	    /*
	     * Clear input stream buffer
	     */
	    try {
	    	parseExecResult(inReader); // parse the output
	    	String line = inReader.readLine();
	    	while(line != null) { 
	    		line = inReader.readLine();
	    	}
	    	exitCode = proc.waitFor();
	    	try {
	    		errThread.join();
	    	} catch (InterruptedException ie) { 
	    		throw new IOException(ie.toString());
	    	}
	    	completed.set(true);
	    	if (exitCode != 0) {
	    		throw new ExitCodeException(exitCode, errMsg.toString());
	    	}
	    } catch (InterruptedException e) {
	    	
	    } finally {
	    	try {
	    		inReader.close();
	    	} catch (IOException ioe) {
	    		
	    	}
	    	if (!completed.get()) {
	    		errThread.interrupt();
	    	}
	    	try {
	    		errReader.close();
	    	} catch (IOException ioe) {
	    		
	    	}
	    	proc.destroy();
	    }
	}

	@Override
	protected String[] getExecString() {
		return this.command;
	}

	@Override
	protected void parseExecResult(BufferedReader lines) throws IOException {
		output = new StringBuffer();
		char[] buf = new char[1024];
		int nRead;
		while ((nRead = lines.read(buf, 0, buf.length)) > 0 ) {
			output.append(buf, 0, nRead);
		}
	}

	public String getOutput() {
		return (output == null) ? "" : output.toString();
	}
}