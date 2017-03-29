import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Properties;
import java.util.Scanner;

/**
 * @author      Krishna Tippur Gururaj
 * @version     1.0
 */

public class PubSubAgent implements Publisher, Subscriber {

	static int userID  = -1 ;
	private static String userType ;

    /**
     * This static class is used to receive notifications from the Event Manager.
     */
	static class receiveFromEM implements Runnable {

		InetAddress ipaddr ;
		int port ;

        /**
         * Constructor method.
         * @param ip: IP address of the subscriber.
         * @param portno: port where the subscriber is listening for incoming connections from the Event Manager.
         */
		public receiveFromEM(InetAddress ip, int portno) {
			ipaddr = ip ;
			port = portno ;
		}

        /**
         * This helper method is used to write appropriate responses back toward the Event Manager.
         * @param out: the objectoutputstream corresponding to the established TCP connection with the Event Manager.
         * @param c: the input from the user.
         * @throws IOException
         */
		public void writeQuestionResponseToStream(ObjectOutputStream out, Character c) throws IOException {
		    if (c.equals('y')) {
		        out.writeInt(1);
		        out.flush();
            }
            else if (c.equals('n')) {
		        out.writeInt(0);
		        out.flush();
            }
        }

		@Override
		public void run() {
			try {
				ServerSocket serverSocket = new ServerSocket(port, 50, ipaddr) ;
				while (true) {
					Socket incoming = serverSocket.accept() ;
					ObjectOutputStream out = new ObjectOutputStream(incoming.getOutputStream()) ;
					ObjectInputStream in = new ObjectInputStream(incoming.getInputStream()) ;
					int type = in.readInt() ;
					Object recvFromEM = in.readObject() ;
					if (recvFromEM instanceof Topic) {
                        Topic recvTopic = (Topic)recvFromEM ;
                        // System.out.println("recv Topic") ;
                        // System.out.println("topic: " + recvTopic.getName()) ;
                        boolean isDone = false ;
                        while (true) {
                            Scanner sc = new Scanner(System.in) ;
                            switch (type) {
                                case 1:
                                    System.out.println("Enter 'y' or 'n'") ;
                                    System.out.println("Do you want to subscribe to " + recvTopic.getName() + "?") ;
                                    Character cSub = sc.next().charAt(0) ;
                                    writeQuestionResponseToStream(out, cSub) ;
                                    isDone = true ;
                                    break ;
                                case 2:
                                    System.out.println("Enter 'y' or 'n'") ;
                                    System.out.println("Do you want to un-subscribe to " + recvTopic.getName() + "?") ;
                                    Character cUnsub = sc.next().charAt(0) ;
                                    writeQuestionResponseToStream(out, cUnsub) ;
                                    isDone = true ;
                                    break ;
                                default:
                                    System.out.println("invalid input!") ;
                                    break ;
                            }
                            if (isDone == true) {
                                break ;
                            }
                            sc.close();
                        }
                        in.close();
                        out.close();
                    }
					else if (recvFromEM instanceof Event) {
						Event recvEvent = (Event)recvFromEM ;
						// System.out.println("recv Event") ;
						System.out.println("topic: " + recvEvent.getTopic().getName()) ;
						System.out.println("title: " + recvEvent.getTitle()) ;
						System.out.println("content: " + recvEvent.getContent()) ;
					}
				}
			} catch (IOException | ClassNotFoundException e) {
				System.err.println("Unable to create server socket at [" + ipaddr.toString() + "], port [" + port + "]") ;
				System.exit(1);
			}
		}
	}

    /**
     * This static class is used to send information toward the Event Manager.
     */
	static class sendToEM implements Runnable {

		InetAddress emIPAddr ;
		int initEMPort ;
		int actualEMPort ;
		int selfPubSubPort ;
		Event event = null ;
		Topic topic = null ;
		ObjectInputStream in = null ;
		ObjectOutputStream out = null ;

        /**
         * Constructor method.
         * @param ip: IP address of the Event Manager.
         * @param port: port of the server socket of the Event Manager.
         * @param obj: object to be sent
         * @param selfPort: port where the client is listening to for TCP connections.
         */
		public sendToEM(InetAddress ip, int port, Object obj, int selfPort) {
            this(ip, port, selfPort) ;
			if (obj instanceof Topic) {
				topic = (Topic) obj ;
			}
			else if (obj instanceof Event) {
				event = (Event) obj ;
			}
			else {
				System.err.println("Unknown obj sent to sendToEM()") ;
			}
		}

		public sendToEM(InetAddress ip, int port, int selfPort) {
		    this(ip, port) ;
			selfPubSubPort = selfPort ;
		}

		public sendToEM(InetAddress ip, int port) {
		    emIPAddr = ip ;
		    initEMPort = port ;
		    selfPubSubPort = 0 ;
        }

		@Override
		public void run() {
			try {
				Socket firstSocket = new Socket(emIPAddr, initEMPort) ;
				// System.out.println("socket created") ;
				out = new ObjectOutputStream(firstSocket.getOutputStream()) ;
				// System.out.println("out created") ;
				in = new ObjectInputStream(firstSocket.getInputStream()) ;
				// System.out.println("in created") ;
				out.writeInt(userID) ;
				// System.out.println("sent user id: " + userID) ;
				out.writeInt(selfPubSubPort) ;
				// System.out.println("sent self port: " + selfPubSubPort) ;
				out.flush() ;
				actualEMPort = in.readInt() ;
				if (actualEMPort == -1) {
				    System.out.println("all server ports are occupied. Try again later.") ;
				    return ;
                }
				// System.out.println("recv port: " + actualEMPort) ;
				in.close() ;
				out.close() ;
				firstSocket.close() ;
				if (event == null && topic == null) {
				    // System.out.println("no need for second socket at " + actualEMPort) ;
					return ; // in case of a subscriber; nothing to send to EM anyway so no need for secondSocket
				}
				Socket secondSocket = new Socket(emIPAddr, actualEMPort) ;
				// System.out.println("second socket created") ;
				out = new ObjectOutputStream(secondSocket.getOutputStream()) ;
				// System.out.println("second out created") ;
                in = new ObjectInputStream(secondSocket.getInputStream()) ;
                // System.out.println("second in created") ;
				out.writeInt(userID) ;
                out.flush() ;
				if (topic != null) {
					out.writeObject(topic) ;
				}
				else if (event != null) {
					out.writeObject(event) ;
				}
				else {
					System.err.println("Error while sending obj") ;
				}
				out.flush() ;
				in.close();
				out.close();
				// System.out.println("closing connection with port " + actualEMPort) ;
				secondSocket.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	@Override
	public void subscribe(Topic topic) {
		// TODO Auto-generated method stub

	}

	@Override
	public void subscribe(String keyword) {
		// TODO Auto-generated method stub

	}

	@Override
	public void unsubscribe(Topic topic) {
		// TODO Auto-generated method stub

	}

	@Override
	public void unsubscribe() {
		// TODO Auto-generated method stub

	}

	@Override
	public void listSubscribedTopics() {
		// TODO Auto-generated method stub

	}

	@Override
	public void publish(Event event) {
		// TODO Auto-generated method stub

	}

	@Override
	public void advertise(Topic newTopic) {
		// TODO Auto-generated method stub

	}

    /**
     * Helper method to create default config file for operation.
     * @param args: command line arguments given to the main().
     */
	public static void preprocessing(String args[]) {

        Properties prop = new Properties() ;
        FileOutputStream out = null ;

        String configFile = args[0] + ".config.properties";
        if (args[0].equals("subscriber")) {
            configFile = args[0] + args[1] + ".config.properties" ;
        }

        try {
            out = new FileOutputStream(configFile) ;
            prop.setProperty("selfipaddr", "localhost");
            if (args[0].equals("subscriber")) {
                prop.setProperty("selfport", Integer.toString(8000 + Integer.parseInt(args[1]))) ;
            }
            if (args[0].equals("subscriber")) {
                prop.setProperty("userid", args[1]) ;
            }
            else if (args[0].equals("publisher")) {
                prop.setProperty("userid", "0");
            }
            if (args[0].equals("publisher")) {
                prop.setProperty("topic", "football") ;
                prop.setProperty("content", "manutd") ;
                prop.setProperty("title", "ggmu") ;
            }
			prop.setProperty("emipaddr", "localhost") ;
			prop.setProperty("emport", "30097") ;
			String comments = "This sample file is for " + args[0] ;
			prop.store(out, comments);
		} catch (IOException e) {
			e.printStackTrace();
		}
		finally {
			if (out != null) {
				try {
					out.close() ;
				} catch (IOException e) {
					e.printStackTrace() ;
				}
			}
		}
	}

	public static void main(String args[]) {

		switch (args.length) {
            case 0:
                System.err.println("USAGE: java PubSubAgent <publisher/subscriber> <optional args ...>") ;
                System.exit(2);
            case 1:
                if (args[0].equals("subscriber")) {
                    System.err.println("USAGE: java PubSubAgent <publisher/subscriber> <user id>") ;
                    System.exit(2);
                }
                else if (args[0].equals("publisher")) {
                    System.err.println("USAGE: java PubSubAgent <publisher/subscriber> <advertise/publish>") ;
                    System.exit(2);
                }
                else {
                    System.err.println("Invalid input!") ;
                    System.exit(2);
                }
                break ;
            default:
                break ;
        }

		Properties prop = new Properties() ;
		FileInputStream in = null ;
		String configFile = args[0] + ".config.properties";
		if (args[0].equals("subscriber")) {
		    configFile = args[0] + args[1] + ".config.properties" ;
        }
		try {
            // in = new FileInputStream("config.properties") ;
            in = new FileInputStream(configFile) ;
		}
		catch (IOException e) {
			System.err.println("Unable to access " + configFile + " file from project root folder. Creating it now ..") ;
			System.out.println("Once the file is created, please edit " + configFile + " and re-run PubSubAgent") ;
			preprocessing(args) ;
            System.exit(0);
		}
		finally {
			if (in != null) {
				try {
					in.close() ;
				} catch (IOException e) {
					e.printStackTrace() ;
				}
			}
		}

		try {
			in = new FileInputStream(configFile) ;
			prop.load(in);
			InetAddress selfIP = InetAddress.getByName(prop.getProperty("selfipaddr")) ;
			InetAddress emIP = InetAddress.getByName(prop.getProperty("emipaddr")) ;
			int emPort = Integer.parseInt(prop.getProperty("emport")) ;
            userID = Integer.parseInt(prop.getProperty("userid")) ;
            userType = args[0] ;
            System.out.println("started as: " + userType) ;
            if (userType.equals("publisher")) {
                if (args[1].equals("advertise")) {
                    Topic newTopic = new Topic() ;
                    newTopic.setName(prop.getProperty("topic"));
                    Thread sendThread = new Thread(new sendToEM(emIP, emPort, newTopic, 0)) ;
                    sendThread.start() ;
                }
                else { // publishing by default
                    Event newEvent = new Event() ;
                    Topic newTopic = new Topic() ;
                    newTopic.setName(prop.getProperty("topic"));
                    newEvent.setTopic(newTopic);
                    newEvent.setContent(prop.getProperty("content"));
                    newEvent.setTitle(prop.getProperty("title"));
                    Thread sendThread = new Thread(new sendToEM(emIP, emPort, newEvent, 0)) ;
                    sendThread.start() ;
                }

            }

			if (userType.equals("subscriber")) {
                int selfPort = Integer.parseInt(prop.getProperty("selfport")) ;
				Thread recvThread = new Thread(new receiveFromEM(selfIP, selfPort)) ;
				recvThread.start() ;

				Thread sendThread = new Thread(new sendToEM(emIP, emPort, selfPort)) ;
				sendThread.start() ;
			}
			else if (userType.equals("publisher")) {
				//TODO: see if anything needs to be done here. Maybe not.
			}
		}
		catch (IOException e) {
			System.err.println(configFile + " file does not exist!") ;
			System.exit(1) ;
		}
	}

}
