package com.hyphenate.mqttdemo;

import android.os.Bundle;
import android.view.View;

import androidx.annotation.Nullable;
import androidx.appcompat.app.AppCompatActivity;

import org.eclipse.paho.mqttv5.client.IMqttToken;
import org.eclipse.paho.mqttv5.client.MqttCallback;
import org.eclipse.paho.mqttv5.client.MqttClient;
import org.eclipse.paho.mqttv5.client.MqttConnectionOptions;
import org.eclipse.paho.mqttv5.client.MqttDisconnectResponse;
import org.eclipse.paho.mqttv5.client.persist.MemoryPersistence;
import org.eclipse.paho.mqttv5.common.MqttException;
import org.eclipse.paho.mqttv5.common.MqttMessage;
import org.eclipse.paho.mqttv5.common.packet.MqttProperties;
import org.eclipse.paho.mqttv5.common.packet.MqttReturnCode;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class MainActivity extends AppCompatActivity {
    String token = "YWMte0kxMjJdEeyA86Vs2wC39E2lU4AgxUF2tgaltfGLJq5UTAyg9egR64SsH92WoZvVAwMAAAF8on5bIjht7EA5Vzt3ez3RpL9W_1xo1r6SJlYiz5nIGUqGLYunYWr16g";
    String deviceId = "test2";
    String appId = "xttij0";
    /**
     * 设置接入点，进入console管理平台获取
     */
    String endpoint = "xttij0.cn1.mqtt.chat";

    /**
     * MQTT客户端ID，由业务系统分配，需要保证每个TCP连接都不一样，保证全局唯一，如果不同的客户端对象（TCP连接）使用了相同的clientId会导致连接异常断开。
     * clientId由两部分组成，格式为DeviceID@appId，其中DeviceID由业务方自己设置，appId在控console控制台创建，，clientId总长度不得超过64个字符。
     */
    String clientId = deviceId + "@" + appId;

    /**
     * 需要订阅或发送消息的topic名称
     * 如果使用了没有创建或者没有被授权的Topic会导致鉴权失败，服务端会断开客户端连接。
     */
    final String myTopic = "myTopic";

    /**
     * QoS参数代表传输质量，可选0，1，2。详细信息，请参见名词解释。
     */
    final int qosLevel = 1;
    final MemoryPersistence memoryPersistence = new MemoryPersistence();

    /**
     * 客户端协议和端口。客户端使用的协议和端口必须匹配，如果是ws或者wss使用http://，如果是mqtt或者mqtts使用tcp://
     */
    org.eclipse.paho.mqttv5.client.MqttClient mqttClient;
    @Override
    protected void onCreate(@Nullable Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_main);

        setMqtt();
    }

    /**
     * Start a new thread
     */
    private void setMqtt() {
        new Thread(){
            public void run(){
                try {
                    mqttClient = new MqttClient("tcp://" + endpoint + ":1883", clientId, memoryPersistence);
                } catch (MqttException e) {
                    e.printStackTrace();
                }
                /**
                 * 设置客户端发送超时时间，防止无限阻塞。
                 */
                mqttClient.setTimeToWait(5000);

                final ExecutorService executorService = new ThreadPoolExecutor(1, 1, 0, TimeUnit.MILLISECONDS,
                        new LinkedBlockingQueue<>());

                mqttClient.setCallback(new MqttCallback() {
                    @Override
                    public void disconnected(MqttDisconnectResponse mqttDisconnectResponse) {
                        System.out.println("disconnected: " + mqttDisconnectResponse.getReturnCode() + ", reason:" + mqttDisconnectResponse.getReasonString());
                        //若是相同deviceId设备登录，关闭客户端
                        if (mqttDisconnectResponse.getReturnCode() == MqttReturnCode.RETURN_CODE_SESSION_TAKEN_OVER) {
                            try {
                                mqttClient.close();
                            } catch (MqttException e) {
                                e.printStackTrace();
                            }
                        }
                    }

                    @Override
                    public void mqttErrorOccurred(MqttException e) {
                        e.printStackTrace();
                    }

                    @Override
                    public void messageArrived(String s, MqttMessage mqttMessage) throws Exception {
                        System.out.println("receive msg from topic " + s + " , body is " + new String(mqttMessage.getPayload()) + ", id 为" + mqttMessage.getId());
                    }

                    @Override
                    public void deliveryComplete(IMqttToken iMqttToken) {
                        System.out.println("send msg succeed topic is : " + iMqttToken.getTopics()[0] + ",id为" + iMqttToken.getMessageId());
                    }

                    @Override
                    public void connectComplete(boolean b, String s) {
                        System.out.println("connect success");
                        executorService.submit(() -> {
                            try {
                                final String[] topicFilter = {myTopic, "topic1"};
                                final int[] qos = {qosLevel, qosLevel};
                                mqttClient.subscribe(topicFilter, qos);
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        });
                    }

                    @Override
                    public void authPacketArrived(int i, MqttProperties mqttProperties) {

                    }
                });
                MqttConnectionOptions mqttConnectOptions = new MqttConnectionOptions();
                /**
                 * 用户名，在console中注册
                 */
                mqttConnectOptions.setUserName("test1");
                /**
                 * 用户密码为第一步中申请的token
                 */
                mqttConnectOptions.setPassword(token.getBytes(StandardCharsets.UTF_8));
                mqttConnectOptions.setKeepAliveInterval(90);
                mqttConnectOptions.setAutomaticReconnect(true);
                mqttConnectOptions.setConnectionTimeout(5000);

                try {
                    mqttClient.connect(mqttConnectOptions);
                } catch (MqttException e) {
                    e.printStackTrace();
                }
                //暂停1秒钟，等待连接订阅完成
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                for (int i = 0; i < 10; i++) {
                    MqttMessage message = new MqttMessage("hello world pub sub msg".getBytes());
                    message.setQos(qosLevel);
                    /**
                     * 发送普通消息时，Topic必须和接收方订阅的Topic一致，或者符合通配符匹配规则。
                     */
                    try {
                        mqttClient.publish("ttt", message);
                    } catch (MqttException e) {
                        e.printStackTrace();
                    }
                }
                try {
                    Thread.sleep(100000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                try {
                    mqttClient.unsubscribe(new String[]{myTopic});
                } catch (MqttException e) {
                    e.printStackTrace();
                }
            }
        }.start();

    }

    public void conClick(View view) {

    }

    public void subClick(View view) {

    }

    public void sendClick(View view) {

    }
}
