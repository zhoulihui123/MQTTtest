
import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import java.util.stream.IntStream;


public class MyMqttClient  {

    public static MqttClient mqttClient = null;
    private static MemoryPersistence memoryPersistence = null;
    private static MqttConnectOptions mqttConnectOptions = null;
    private static final String BROKER = "tcp://10.202.40.162:1883"; // EMQ X Broker 地址
    static int succeedsum = 0;
    static int succeedconnectsum = 0;
    private static String ClientName = "emqx_ekuiper";				//待填	将在服务端出现的名字
    private static String IP = "10.202.40.162";						//待填  服务器IP

    public static void main(String[] args) {
        /*start(ClientName);
        int a=10;
        int b=1;
        sendMaxpalleral(a,b);//并行.clients--客户端数量，message，模拟客户端连续发送的信息数
        //sendMax(a,b);//并行.clients--客户端数量，message，模拟客户端连续发送的信息数
        System.out.println("发送完成"+succeedsum+"——"+a*b+"--成功率——"+((double) succeedsum / (a * b) * 100));*/
        //while (true) {sendMaxpalleral(a,b);}
            //sendMax(a,b);//并行.clients--客户端数量，message，模拟客户端连续发送的信息数

        /*for(int i=0;i<1000;i++){
            String a=Integer.toString(i);;
            starttest(a);
        }
        System.out.println("连接完成"+succeedconnectsum);*/

        int totalClients = 100000; // 客户端数量
        simulateClients(totalClients);


    }
    //模拟一个客户端的发送频率和极限--并行发送
    public static void sendMaxpalleral(int clients,int messages) {
        IntStream.range(0, clients)
                .parallel()
                //.limit(3)
                .forEach(i -> {
                    IntStream.range(0, messages)
                            .parallel()
                            .forEach(j -> {
                                publishMessage("/neuron/sss北向/sss/sss",
                                        "{\"temperture\":"+100+"}",
                                        1);
                                try {
                                    Thread.sleep(5);
                                } catch (InterruptedException e) {
                                    throw new RuntimeException(e);
                                }
                            });
                });
    }
    //模拟一个客户端的发送频率和极限--顺序发送
    public static void sendMax(int clients,int messages) {
        IntStream.range(0, clients)
                .parallel()
                //.limit(3)
                .forEach(i -> {
                    for (int j = 0; j < messages; j++) {
                        //publishMessage("/neuron/sss北向/sss/sss", "{temperture:" + i + "}", 1);// 向主题world发送hello World(客户端)
                        publishMessage("/neuron/sss北向/sss/sss",
                                "{\"temperture\":"+100+"}",
                                1);
                        try {
                            Thread.sleep(10);
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    }
                });
    }


    public static void start(String clientId) {
        //初始化连接设置对象
        mqttConnectOptions = new MqttConnectOptions();
        //设置是否清空session,这里如果设置为false表示服务器会保留客户端的连接记录，
        //这里设置为true表示每次连接到服务器都以新的身份连接
        mqttConnectOptions.setCleanSession(true);
        //设置连接超时时间，单位是秒
        mqttConnectOptions.setConnectionTimeout(10);
        //设置持久化方式
        memoryPersistence = new MemoryPersistence();
        if(null != clientId) {
            try {
                mqttClient = new MqttClient("tcp://"+IP+":1883", clientId,memoryPersistence);
            } catch (MqttException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        System.out.println("连接状态："+mqttClient.isConnected());
        //设置连接和回调
        if(null != mqttClient) {
            if(!mqttClient.isConnected()) {
                //创建回调函数对象
                MQTTReceiveCallback MQTTReceiveCallback = new MQTTReceiveCallback();
                //客户端添加回调函数
                mqttClient.setCallback(MQTTReceiveCallback);
                //创建连接
                try {
                    System.out.println("创建连接");
                    mqttClient.connect(mqttConnectOptions);
                } catch (MqttException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }

            }
        }else {
            System.out.println("mqttClient为空");
        }
        System.out.println("连接状态"+mqttClient.isConnected());
    }

    //	关闭连接
    public void closeConnect() {
        //关闭存储方式
        if(null != memoryPersistence) {
            try {
                memoryPersistence.close();
            } catch (MqttPersistenceException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }else {
            System.out.println("memoryPersistence is null");
        }

        //关闭连接
        if(null != mqttClient) {
            if(mqttClient.isConnected()) {
                try {
                    mqttClient.disconnect();
                    mqttClient.close();
                } catch (MqttException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }else {
                System.out.println("mqttClient is not connect");
            }
        }else {
            System.out.println("mqttClient is null");
        }
    }

    //	发布消息
    public static void publishMessage(String pubTopic, String message, int qos) {
        if(null != mqttClient&& mqttClient.isConnected()) {
            //System.out.println("发布消息   "+mqttClient.isConnected());
           // System.out.println("id:"+mqttClient.getClientId());
            MqttMessage mqttMessage = new MqttMessage();
            mqttMessage.setQos(qos);
            mqttMessage.setPayload(message.getBytes());

            MqttTopic topic = mqttClient.getTopic(pubTopic);

            if(null != topic) {
                try {
                    MqttDeliveryToken publish = topic.publish(mqttMessage);
                    if(!publish.isComplete()) {
                            succeedsum++;
                         System.out.println("消息发布成功");
                    }
                } catch (MqttException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        }else {
            reConnect();
        }
    }

    //	重新连接
    public static void reConnect() {
        if(null != mqttClient) {
            if(!mqttClient.isConnected()) {
                if(null != mqttConnectOptions) {
                    try {
                        mqttClient.connect(mqttConnectOptions);
                    } catch (MqttException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                }else {
                    System.out.println("mqttConnectOptions is null");
                }
            }else {
                System.out.println("mqttClient is null or connect");
            }
        }else {
            start(ClientName);
        }
    }
    //	订阅主题
    public static void subTopic(String topic) {
        if(null != mqttClient&& mqttClient.isConnected()) {
            try {
                mqttClient.subscribe(topic, 1);
            } catch (MqttException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }else {
            System.out.println("mqttClient is error");
        }
    }

    //	清空主题
    public void cleanTopic(String topic) {
        if(null != mqttClient&& !mqttClient.isConnected()) {
            try {
                mqttClient.unsubscribe(topic);
            } catch (MqttException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }else {
            System.out.println("mqttClient is error");
        }
    }



        public static void simulateClients(int totalClients) {
            for (int i = 1; i <= totalClients; i++) {
                String clientId = "client" + i;

                try {
                    MqttClient client = new MqttClient(BROKER, clientId, new MemoryPersistence());

                    MqttConnectOptions options = new MqttConnectOptions();
                    options.setCleanSession(true);

                    client.connect(options);
                    System.out.println("客户端 " + clientId + " 连接成功");

                    // 可以在这里执行其他操作，如发布和订阅消息等

                    /*client.disconnect();
                    System.out.println("客户端 " + clientId + " 断开连接");*/

                } catch (MqttException e) {
                    e.printStackTrace();
                }
            }
        }


    }

