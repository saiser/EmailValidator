package com.saiser.email_validator;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.validator.EmailValidator;
import org.xbill.DNS.Lookup;
import org.xbill.DNS.Record;
import org.xbill.DNS.TextParseException;
import org.xbill.DNS.Type;

import rx.Observable;


public class EmailValidatorService {
	
	private static final int DEFAULT_SMTP_PORT = 25;
	
	private static final String DEFAULT_SENDER_EMAIL = "noreplay@gtdollar.com";
	
	private static final int DEFAULT_SOCKET_TIMEOUT = 3000;
	
	private static final String MESSAGE_HELO = "HELO";
	
	private static final String MESSAGE_MAIL_FROM = "mail from:<%s>";
	
	private static final String MESSAGE_RCPT_TO = "rcpt to:<%s>";
	
	private static final ExecutorService exe = new ThreadPoolExecutor(Runtime.getRuntime().availableProcessors(), Runtime.getRuntime().availableProcessors() * 2, 10, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(10000));
	
	public enum EmailStatus {
		INVALID,
		ERROR,
		NO_MX_RECORDS,
		EXIST,
		NOT_EXIST
	}
	
	// just for test
	public static void main(String[] args) {
		EmailValidatorService evs = new EmailValidatorService();
		String[] domains = new String[] {"out.com", "outlook.com", "test.com", "21.vc", "qq.com", "yeah.net", "126.com", "163.com", "google.com", "gtdollar.com", "buokdsf.co", "gmail.com"};
		String[] names = new String[] {"saiser", "9339644", "ma", "remote", "mama", "yl", "123", "fayc", "shenyuliang.saiser", "shangyu", "hel.o@"};
		for (String domain : domains) {
			for (String name : names) {
				String email = name + "@" + domain;
				evs.checkIfEmailExists(email).subscribe(ret -> {
					System.out.println("[check_email] " + email + "->" + ret);
				}, error -> {
					System.out.println(error.getMessage());
				});
			}
		}
	}
	
	private Observable<List<String>> sendHeloMessage(PrintWriter writer, BlockingQueue<String> bq) throws Exception {
		return Observable.<List<String>>create(subscriber -> {
			List<String> ret = new ArrayList<String>();
			try {
				String mxHostString = bq.poll(DEFAULT_SOCKET_TIMEOUT, TimeUnit.MILLISECONDS);
				String message = MESSAGE_HELO;
				if (mxHostString != null) {
					String[] mxHostinfo = mxHostString.split(" ");
					if (mxHostinfo != null && mxHostinfo.length >= 2) {
						message += " " + mxHostinfo[1];
					} 
				}
				
				writer.println(message);
				String r = bq.poll(DEFAULT_SOCKET_TIMEOUT, TimeUnit.MILLISECONDS);
				ret.add(r);
			} catch (Exception e) {
				subscriber.onError(e);
			}
			subscriber.onNext(ret);
		});
	}
	
	private Observable<List<String>> sendMailFromMessage(PrintWriter writer, BlockingQueue<String> bq){
		return Observable.<List<String>>create(subscriber -> {
			List<String> ret = new ArrayList<String>();
			try {
				bq.clear();
				String message = String.format(MESSAGE_MAIL_FROM, DEFAULT_SENDER_EMAIL);
				writer.println(message);
				String r = bq.poll(DEFAULT_SOCKET_TIMEOUT, TimeUnit.MILLISECONDS);
				ret.add(r);
			} catch (Exception e) {
				e.printStackTrace();
			}
			subscriber.onNext(ret);
		});
		
	}
	
	private Observable<List<String>> sendRcptToMessage(PrintWriter writer, BlockingQueue<String> bq, String email){
		return Observable.<List<String>>create(subscriber -> {
			List<String> ret = new ArrayList<String>();
			try {
				bq.clear();
				String message = String.format(MESSAGE_RCPT_TO, email);
				writer.println(message);
				String r = bq.poll(DEFAULT_SOCKET_TIMEOUT, TimeUnit.MILLISECONDS);
				ret.add(r);
			} catch (Exception e) {
				e.printStackTrace();
			}
			subscriber.onNext(ret);
		});
		
	}
	
	private Record[] lookupMxRecords(final String domainPart) throws TextParseException {
	    final Lookup dnsLookup = new Lookup(domainPart, Type.MX);
	    return dnsLookup.run();
	}
	
	private void readData(BufferedReader in, BlockingQueue<String> bq, long timeout, List<Future<?>> future) throws IOException, InterruptedException {
		Future<?> f = exe.submit(new Runnable() {
			
			@Override
			public void run() {
				try {
					for (String output = in.readLine(); output != null; output = in.readLine()) {
						bq.put(output);
					}
				} catch (Exception ignore) {
					//log.error(ignore.getMessage(), ignore);
				}
			}
		});
		future.add(f);
	}
	
	/**
	 * if result is NOT_EXIST, NO_MX_RECORDS and INVALID, can choose not send email
	 * @param email
	 * @return
	 */
	public Observable<EmailStatus> checkIfEmailExists(String email) {
		return Observable.create(subscriber -> {
			try {
				if (!EmailValidator.getInstance().isValid(email)) {
					subscriber.onNext(EmailStatus.INVALID);
					return;
				}
				String domain = email.substring(email.indexOf("@") + 1);
				Record[] records = lookupMxRecords(domain);
				if (records == null || records.length == 0) {
					subscriber.onNext(EmailStatus.NO_MX_RECORDS);
					return;
				}
				Socket socket = new Socket(records[0].getAdditionalName().toString(), DEFAULT_SMTP_PORT);
				PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
			    BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
		        try {
		        	BlockingQueue<String> bq = new ArrayBlockingQueue<String>(100);
		        	List<Future<?>> future = new ArrayList<Future<?>>();
		            readData(in, bq, DEFAULT_SOCKET_TIMEOUT, future);
		            sendHeloMessage(out, bq).subscribe(hm -> {
			        	for (String hmItem : hm) {
			        		try {
			        			Integer hms = Integer.parseInt(hmItem.split(" ")[0].substring(0, 3));
			        			if (hms < 200 || hms >= 300) {
			        				closeSocket(socket);
				        			subscriber.onNext(EmailStatus.ERROR);
				        			return;
				        		} 
			        		} catch(Exception ex) {
			        			closeSocket(socket);
			        			subscriber.onNext(EmailStatus.ERROR);
			        			return;
			        		}
			        		
			        		sendMailFromMessage(out, bq).subscribe(mfm -> {
			        			for (String mfmItem : mfm) {
			        				try {
			        					Integer mfms = Integer.parseInt(mfmItem.split(" ")[0].substring(0, 3));
				        				if (mfms < 200 || mfms >= 300) {
				        					closeSocket(socket);
				        					subscriber.onNext(EmailStatus.ERROR);
				        					return;
						        		} 
			        				} catch (Exception ex) {
			        					closeSocket(socket);
			        					subscriber.onNext(EmailStatus.ERROR);
			        					return;
			        				}
			        				
			        				sendRcptToMessage(out, bq, email).subscribe(rtm -> {
			        					for (String rtmItem : rtm) {
			        						try {
			        							Integer rtms = Integer.parseInt(rtmItem.split(" ")[0].substring(0, 3));
				        						if (rtms < 200 || rtms >= 300) {
				        							closeSocket(socket);
								        			subscriber.onNext(EmailStatus.NOT_EXIST);
								        			return;
								        		} 
			        						} catch (Exception ex) {
			        							closeSocket(socket);
			        							subscriber.onNext(EmailStatus.ERROR);
			        							return;
			        						}
			        					}
			        					closeSocket(socket);
			        					subscriber.onNext(EmailStatus.EXIST); // success
			        					return;
			        				}, error -> {
			        					closeSocket(socket);
			        					subscriber.onNext(EmailStatus.ERROR);
			        					return;
			        				});
			        			}
			        		}, error -> {
			        			closeSocket(socket);
			        			subscriber.onNext(EmailStatus.ERROR);
			        			return;
			        		});
			        	}
			        }, error -> {
			        	closeSocket(socket);
			        	subscriber.onNext(EmailStatus.ERROR);
			        	return;
			        });
		        } catch (Exception ex) {
		        	subscriber.onNext(EmailStatus.ERROR);
		        } finally {
		        	out.close();
		        	in.close();
		        	if (!socket.isClosed()) {
		        		socket.close();
		        	}
		        }
			} catch (Exception ex) {
				//log.error("[check_if_email_exists] " + ex.getMessage(), ex);
				subscriber.onNext(EmailStatus.ERROR);
			}
		});
	}
	
	private void closeSocket(Socket socket) {
		try {
			if (!socket.isInputShutdown())
				socket.shutdownInput();
			if (!socket.isOutputShutdown())
				socket.shutdownOutput();
		} catch (Exception ex) {
			
		}
	}
}
