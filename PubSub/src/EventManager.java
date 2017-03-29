import java.io.*;
import java.net.BindException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author      Krishna Tippur Gururaj
 * @version     1.0
 */

public class EventManager {

    /**
     * This static class is to be used for sending any outbound information toward Subscribers.
     */

    static class handleOutgoingConn implements Runnable {

        ArrayList<Integer> recipients = null ;
        Topic topicToBeSent = null ;
        Event eventToBeSent = null ;
        int subscribeUnsubscribe ; // 1 = ask for subscribe, 2 = ask for unsubscribe, 0 = for sending out events

        public handleOutgoingConn(ArrayList<Integer> list, Object obj) {
            if (obj instanceof Topic) {
                topicToBeSent = (Topic)obj ;
            }
            else if (obj instanceof Event) {
                eventToBeSent = (Event)obj ;
            }
            else {
                System.err.println("FATAL: Incorrect obj sent to constructor of handleOutgoingConn") ;
                return ;
            }
            recipients = list ;
            subscribeUnsubscribe = 0 ;
        }

        /**
         * Constructor method.
         * @param list: list of recipient user IDs.
         * @param obj: object to be sent to the list of recipients.
         * @param type: parameter for setting mode of usage.
         */
        public handleOutgoingConn(ArrayList<Integer> list, Object obj, int type) {
            this(list, obj) ;
            subscribeUnsubscribe = type ;
        }

        /**
         * This method connects and sends information toward a subscriber.
         * @param ip: IP address of remote client
         * @param port: Server port of remote client
         * @param obj: The topic or event that is to be sent.
         * @param userID: unique identifier of the remote client.
         */
        public void connectAndSend(InetAddress ip, int port, Object obj, int userID) {
            try {
                Socket socket = new Socket(ip, port) ;
                ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream()) ;
                ObjectInputStream in = new ObjectInputStream(socket.getInputStream()) ;
                out.writeInt(subscribeUnsubscribe); // 0 -> event, 1 -> ask for subscribe, 2 -> ask for un-subscribe
                out.writeObject(obj) ;
                out.flush() ;
                Thread.sleep(1000);
                if (pendingSubEvents.containsKey(userID)) {
                    pendingSubEvents.get(userID).remove(obj) ;
                    // System.out.println("sent and removed") ;
                }
                if (obj instanceof Topic) { // this is in the case of advertise(). Wait for confirmation.
                    // System.out.println("connectAndSend for userID: " + userID) ;
                    Integer choice = in.readInt() ;
                    switch (subscribeUnsubscribe) {
                        case 1: // for subscribe
                            if (choice == 1) { // subscriber wants to subscribe
                                int key = allTopics.indexOf(((Topic) obj).getName()) ;
                                ArrayList<Integer> allSubsOfTopic = allTopicSubscribers.get(key) ;
                                allSubsOfTopic.add(userID) ;
                                if (!allSubTopics.get(userID).contains(obj)) {
                                    allSubTopics.get(userID).add((Topic)obj) ;
                                    System.out.println("topic [" + ((Topic) obj).getName() + "] subscribed by subID: " + userID) ;
                                }
                                else {
                                    System.out.println("User " + userID + " has already subscribed to " + ((Topic) obj).getName()) ;
                                }
                                // allTopicSubscribers.put(userID, allSubsOfTopic) ; // may not be needed; uncomment if reqd.
                            }
                            else if (choice == 0) { // does not want to subscribe
                                System.out.println("topic [" + ((Topic) obj).getName() + "] not subscribed by subID: " + userID) ;
                            }
                            else {
                                System.err.println("FATAL: Unexpected int received! [" + choice + "]") ;
                            }
                            break ;
                        case 2: // for un-subscribe
                            if (choice == 1) { // subscriber wants to un-subscribe
                                int key = allTopics.indexOf(((Topic) obj).getName()) ;
                                ArrayList<Integer> allSubsOfTopic = allTopicSubscribers.get(key) ;
                                allSubsOfTopic.remove(key) ;
                                allSubTopics.get(userID).remove(obj) ;
                                System.out.println("topic [" + ((Topic) obj).getName() + "] un-subscribed by subID: " + userID) ;
                            }
                            else if (choice == 0) { // does not want to unsubscribe
                                System.out.println("topic [" + ((Topic) obj).getName() + "] still subscribed by subID: " + userID) ;
                            }
                            else {
                                System.err.println("FATAL: Unexpected int received! [" + choice + "]") ;
                            }
                            break ;
                        default:
                                System.err.println("Should never be here!! EVER!") ;
                                break ;
                    }
                }
                out.close() ;
            } catch (IOException e) {
                System.out.println("User ID: " + userID + " is not listening on IP: " + ip + " and port: " + port) ;
                if (obj instanceof Event) {
                    // System.out.println("adding to pending list") ;
                    pendingSubEvents.computeIfAbsent(userID, k -> new ArrayList<>());
                    if (!pendingSubEvents.get(userID).contains(obj)) {
                        pendingSubEvents.get(userID).add((Event) obj) ;
                        // System.out.println("added to userID: " + userID) ;
                    }
                }
                // e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        public void run() {
            if (recipients == null) {
                System.err.println("FATAL: not running thread because constructor failed") ;
                return ;
            }
            // System.out.println("size: " + recipients.size()) ;
            for (int i = 0 ; i < recipients.size() ; i++) {
                int index = recipients.get(i) ;
                SubscriberInfo subInfo = subDetails.get(index) ;
                if (topicToBeSent != null) {
                    connectAndSend(subInfo.getIPAddr(), subInfo.getPort(), topicToBeSent, index);
                }
                else if (eventToBeSent != null) {
                    connectAndSend(subInfo.getIPAddr(), subInfo.getPort(), eventToBeSent, index);
                    if ((pendingSubEvents.containsKey(index)) && (pendingSubEvents.get(index).size() > 0)) {
                        ArrayList<Event> pendingEvents = pendingSubEvents.get(index) ;
                        for (int j = 0 ; j < pendingEvents.size() ; j++) {
                            connectAndSend(subInfo.getIPAddr(), subInfo.getPort(), pendingEvents.get(j), index);
                        }
                    }
                }
                else {
                    System.err.println("FATAL: Should never reach here!") ;
                }
            }
        }

    }

    /**
     * This static class handles the incoming connections from the clients.
     */
    static class handleIncomingPubSubConnThread implements Runnable {

        int port ;
        ServerSocket selfSocket = null ;
        ObjectInputStream inStream = null ;
        ObjectOutputStream outStream = null ;
        boolean isServerListenRequired ;

        /**
         * Constructor method.
         * @param newPort: listen port for server.
         * @param flag: boolean value to determine if the server socket is required.
         * @throws IOException
         */
        public handleIncomingPubSubConnThread(int newPort, boolean flag) throws IOException {
            port = newPort ;
            // System.out.println("port: " + port) ;
            selfSocket = new ServerSocket(port) ;
            isServerListenRequired = flag ;
        }

        /**
         * This method is used for managing the advertise() flow.
         * @param newTopic: topic to be advertised.
         * @param type: parameter to be passed to handleOutgoingConn() thread.
         */
        public void handleAdvertise(Topic newTopic, int type) {
            if (!allTopics.contains(newTopic.getName())) {
                allTopics.add(newTopic.getName()) ;
                allTopicSubscribers.put(allTopics.indexOf(newTopic.getName()), new ArrayList<>()) ;
                // below line retrieves set of all registered subscribers
                ArrayList<Integer> allSubs = new ArrayList<>(subDetails.keySet()) ;
                // System.out.println("allSubs' size: " + allSubs.size()) ;
                // System.out.println("advertise to begin ..") ;
                //TODO: send new topic name to all the subs in arraylist
                Thread t = new Thread(new handleOutgoingConn(allSubs, newTopic, type))  ;
                t.start() ;
                // calling join() to ensure this thread does not finish until the sub-thread is completed.
                try {
                    t.join() ;
                } catch (InterruptedException e) {
                    e.printStackTrace() ;
                }
            }
            else {
                System.out.println("Topic " + newTopic.getName() + " already present") ;
            }
        }

        /**
         * This method is used for managing the publish() flow.
         * @param newEvent: event to be published.
         */
        public void handlePublish(Event newEvent) {
            int key = allTopics.indexOf(newEvent.getTopic().getName()) ;
            if (key == -1) {
                System.err.println("FATAL: topic " + newEvent.getTopic() + " does not exist in memory!") ;
                return ;
            }
            // System.out.println("publish to begin ..") ;
            ArrayList<Integer> allSubsForCurrentTopic = allTopicSubscribers.get(key) ;
            Thread t = new Thread(new handleOutgoingConn(allSubsForCurrentTopic, newEvent)) ;
            t.start() ;
            // calling join() to ensure this thread does not finish until the sub-thread is completed.
            try {
                t.join() ;
            } catch(InterruptedException e) {
                e.printStackTrace();
            }
        }

        public void run() {
            try {
                if (!isServerListenRequired) {
                    // System.out.println("remote is subscriber so no need for this server socket") ;
                    if (portsInUse.contains(port)) {
                       //  System.out.println("removing port " + port + " from arraylist") ;
                        portsInUse.remove(portsInUse.indexOf(port)) ;
                        // System.out.println("also closing socket") ;
                        selfSocket.close();
                    }
                    return;
                }
                // System.out.println("Listen port started at " + port) ;
                Socket tempSocket = selfSocket.accept() ;
                outStream = new ObjectOutputStream(tempSocket.getOutputStream()) ;
                inStream = new ObjectInputStream(tempSocket.getInputStream()) ;
                int incomingID = inStream.readInt() ;
                // System.out.println("incoming id: " + incomingID) ;
                if (incomingID == 0) { // publisher
                    Object incObj = inStream.readObject() ;
                    if (incObj instanceof Topic) { // advertise()
                        System.out.println("publisher invoked advertise") ;
                        handleAdvertise((Topic)incObj, 1) ;
                    }
                    else if (incObj instanceof Event) { // publish()
                        Event incEvent = (Event) incObj ;
                        if (allTopics.contains(incEvent.getTopic().getName())) {
                            System.out.println("publisher invoked publish") ;
                            handlePublish(incEvent) ;
                        }
                        else {
                            System.out.println("Topic " + incEvent.getTopic().getName() + " is not present hence advertising first") ;
                            Topic newTopic = new Topic() ;
                            newTopic.setName(incEvent.getTopic().getName());
                            handleAdvertise(newTopic, 1);
                        }
                    }
                }
            } catch (IOException | ClassNotFoundException e) {
                e.printStackTrace() ;
            }
            if (portsInUse.contains(port)) {
                // System.out.println("removing port " + port + " from arraylist") ;
                if (selfSocket.isBound()) {
                    try {
                        selfSocket.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
                portsInUse.remove(portsInUse.indexOf(port)) ;
            }
        }
    }

    int[] portOptions = {3000, 4000, 5000, 6000, 7000} ; /* configurable */
    static ArrayList<Integer> portsInUse = null ;
    int portCounter = portOptions.length ;
    ObjectInputStream inStream = null ;
    ObjectOutputStream outStream = null ;
    ExecutorService executor = null ;
    // allTopics holds all topics.
    static ArrayList<String> allTopics = null ;
    // index of allTopics is the key for allTopicSubscribers; key corresponds to "topic", the value stored is the list of subscribers.
    static HashMap<Integer, ArrayList<Integer>> allTopicSubscribers = null ;
    // key is the userID, value stored is the list of topics a user has subscribed to.
    static HashMap<Integer, ArrayList<Topic>> allSubTopics = null ;
    static HashMap<Integer, SubscriberInfo> subDetails = null ;
    static HashMap<Integer, ArrayList<Event>> pendingSubEvents = null ;

    public EventManager() {
        executor = Executors.newFixedThreadPool(portOptions.length) ;
        allTopics = new ArrayList<>() ;
        allTopicSubscribers = new HashMap<>() ;
        subDetails = new HashMap<>() ;
        allSubTopics = new HashMap<>() ;
        portsInUse = new ArrayList<>() ;
        pendingSubEvents = new HashMap<>() ;
    }

    /**
     * This method is used to return a free server port to a client for it to connect to thereafter.
     * @return integer value of a free server port.
     */
    public int getRandomPort() {

        if (portsInUse.size() == portOptions.length) {
            System.err.println("all ports currently in use") ;
            return -1 ;
        }

        while (true) {
            if (portCounter == 0) {
                portCounter = portOptions.length - 1 ;
            }
            else {
                portCounter -= 1 ;
            }
            if (!portsInUse.contains(portCounter)) {
                break ;
            }
        }

        int retPort = portOptions[portCounter] ;
        portsInUse.add(retPort) ;

        return retPort ;
    }

    /*
     * Start the repo service
     */
    private void startService(int commonServerPort) {
        Socket incomingSocket = null ;
        ServerSocket servSocket = null ;

        try {
            servSocket = new ServerSocket(commonServerPort) ;
        } catch (IOException e) {
            System.err.println("Unable to open common server port [" + commonServerPort + "]!") ;
            e.printStackTrace() ;
            System.exit(1) ;
        }

        System.out.println("Event manager service started at " + commonServerPort) ;

        while (true) {
            try {
                incomingSocket = servSocket.accept() ;
                outStream = new ObjectOutputStream(incomingSocket.getOutputStream()) ;
                inStream = new ObjectInputStream(incomingSocket.getInputStream()) ;
                int subId = inStream.readInt() ;
                int subPort = inStream.readInt() ;
                if (subId != 0) { // subscriber
                    System.out.println("recv sub id: " + subId) ;
                    if (!subDetails.containsKey(subId)) { // new subscriber connecting for the first time
                        System.out.println("adding subscriber " + subId) ;
                        addSubscriber(incomingSocket, subId, subPort);
                    }
                    else { // for an existing subscriber, see if they want to change subscriptions
                        ArrayList<Integer> sub = new ArrayList<>(1) ;
                        sub.add(subId) ;
                        // first, check about current subscriptions; does a subscriber want to un-subscribe from any?
                        ArrayList<Topic> currentSubscribedTopics = allSubTopics.get(subId) ;
                        for (int i = 0 ; i < currentSubscribedTopics.size() ; i++) {
                            Thread existingSub = new Thread(new handleOutgoingConn(sub, currentSubscribedTopics.get(i), 2)) ;
                            existingSub.start();
                            existingSub.join();
                        }
                        // now check for topics to be subscribed
                        ArrayList<Topic> subscrTopics = allSubTopics.get(subId) ;
                        ArrayList<String> topicStrings = new ArrayList<>(subscrTopics.size()) ;
                        for (int i = 0 ; i < subscrTopics.size() ; i++) {
                            topicStrings.add(subscrTopics.get(i).getName()) ;
                        }
                        for (int i = 0 ; i < allTopics.size() ; i++) {
                            if (!topicStrings.contains(allTopics.get(i))) {
                                // Thread subTopic = new Thread(new handleOutgoingConn(sub, subscrTopics.get(i), 1)) ;
                                Topic newTopic = new Topic() ;
                                newTopic.setName(allTopics.get(i));
                                Thread subTopic = new Thread(new handleOutgoingConn(sub, newTopic, 1)) ;
                                subTopic.start();
                                subTopic.join();
                            }
                        }
                    }
                }
                int newPort = getRandomPort() ;
                if (newPort != -1) {
                    if (subId != 0) { // subscriber
                        handleIncomingPubSubConnThread incoming = new handleIncomingPubSubConnThread(newPort, false) ;
                        executor.execute(incoming);
                    }
                    else {
                        handleIncomingPubSubConnThread incoming = new handleIncomingPubSubConnThread(newPort, true) ;
                        executor.execute(incoming);
                    }
                }
                outStream.writeInt(newPort) ;
                outStream.flush() ;
                outStream.close() ;
            } catch (BindException e) {
                System.err.println("port already bound") ;
                e.printStackTrace() ;
            } catch (IOException e) {
                e.printStackTrace() ;
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    /*
     * add subscriber to the internal list
     */
    private void addSubscriber(Socket incomingSocket, int subID, int subPort){
        InetAddress subIP = incomingSocket.getInetAddress() ;
        SubscriberInfo newSub = new SubscriberInfo(subID, subIP, subPort) ;
        System.out.println("subID: " + subID + " IP: " + subIP + " port: " + subPort) ;
        subDetails.put(subID, newSub) ;
        allSubTopics.put(subID, new ArrayList<>()) ;
    }

    /*
     * remove subscriber from the list
     */
    private void removeSubscriber(int subID){
        if (subDetails.containsKey(subID)) {
            subDetails.remove(subID) ;
            allSubTopics.remove(subID) ;
            System.out.println("sub ID: " + subID + " removed") ;
        }
        else {
            System.out.println("sub ID: " + subID + " not present") ;
        }
    }

    /*
     * show the list of subscriber for a specified topic
     */
    private void showSubscribers(Topic topic){
        String topicName = topic.getName() ;
        int index = allTopics.indexOf(topicName) ;
        ArrayList<Integer> subList = allTopicSubscribers.get(index) ;
        System.out.println("Subscribers of topic [" + topicName + "]:") ;
        for (int i = 0 ; i < subList.size() ; i++) {
            SubscriberInfo tempInfo = subDetails.get(i) ;
            System.out.println("SubID: " + i + " IP: " + tempInfo.getIPAddr() + " Port: " + tempInfo.getPort()) ;
        }
    }

    public static void main(String[] args) {
        if (args.length == 0) {
            System.err.println("USAGE: java EventManager <server port>") ;
            System.exit(2);
        }
        new EventManager().startService(Integer.parseInt(args[0])) ;
    }


}

/**
 * This class can hold data about a subscriber.
 */
class SubscriberInfo {
    private int id ;
    private InetAddress ipaddr ;
    private int port ;

    public SubscriberInfo(int subId, InetAddress subIP, int subPort) {
        id = subId ;
        ipaddr = subIP ;
        port = subPort ;
    }

    public int getID() {
        return id ;
    }

    public InetAddress getIPAddr() {
        return ipaddr ;
    }

    public int getPort() {
        return port ;
    }
}
