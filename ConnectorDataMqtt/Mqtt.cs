﻿using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using BlubbFish.Utils.IoT.Events;
using uPLibrary.Networking.M2Mqtt;
using uPLibrary.Networking.M2Mqtt.Messages;

namespace BlubbFish.Utils.IoT.Connector.Data {
  public class Mqtt : ADataBackend, IDisposable {
    private MqttClient client;
    private Thread connectionWatcher;

    public Mqtt(Dictionary<String, String> settings) : base(settings) {
      Console.WriteLine("BlubbFish.Utils.IoT.Connector.Data.Mqtt.Mqtt()");
      Int32 port = 1883;
      if(this.settings.ContainsKey("port")) {
        port = Int32.Parse(this.settings["port"]);
      }
      this.client = new MqttClient(this.settings["server"], port, false, null, null, MqttSslProtocols.None);
      this.ConnectionWatcher();
    }

    #region ConectionManage
    private void ConnectionWatcher() {
      this.connectionWatcher = new Thread(this.ConnectionWatcherRunner);
      this.connectionWatcher.Start();
    }

    private void ConnectionWatcherRunner() {
      while(true) {
        try {
          if(!this.IsConnected) {
            this.Reconnect();
          }
          Thread.Sleep(500);
        } catch(Exception) { }
      }
    }

    private void Reconnect() {
      Console.WriteLine("BlubbFish.Utils.IoT.Connector.Data.Mqtt.Reconnect()");
      if(this.IsConnected) {
        this.Disconnect(true);
      } else {
        this.Disconnect(false);
      }
      this.Connect();
    }

    private void Disconnect(Boolean complete) {
      Console.WriteLine("BlubbFish.Utils.IoT.Connector.Data.Mqtt.Disconnect()");
      this.client.MqttMsgPublishReceived -= this.Client_MqttMsgPublishReceived;
      this.Unsubscripe();
      if(complete) {
        this.client.Disconnect();
      }
    }

    private void Connect() {
      Console.WriteLine("BlubbFish.Utils.IoT.Connector.Data.Mqtt.Connect()");
      this.client.MqttMsgPublishReceived += this.Client_MqttMsgPublishReceived;
      if (this.settings.ContainsKey("user") && this.settings.ContainsKey("pass")) {
        this.client.Connect(Guid.NewGuid().ToString(), this.settings["user"], this.settings["pass"]);
      } else {
        this.client.Connect(Guid.NewGuid().ToString());
      }
      this.Subscripe();
    }
    #endregion

    #region Subscription
    private void Unsubscripe() {
      if(this.settings.ContainsKey("topic")) {
        this.client.Unsubscribe(this.settings["topic"].Split(';'));
      } else {
        this.client.Unsubscribe(new String[] { "#" });
      }
    }

    private void Subscripe() {
      if(this.settings.ContainsKey("topic")) {
        Int32 l = this.settings["topic"].Split(';').Length;
        Byte[] qos = new Byte[l];
        for(Int32 i = 0; i < qos.Length; i++) {
          qos[i] = MqttMsgBase.QOS_LEVEL_AT_LEAST_ONCE;
        }
        this.client.Subscribe(this.settings["topic"].Split(';'), qos);
      } else {
        this.client.Subscribe(new String[] { "#" }, new Byte[] { MqttMsgBase.QOS_LEVEL_AT_LEAST_ONCE });
      }
    }
    #endregion

    private async void Client_MqttMsgPublishReceived(Object sender, MqttMsgPublishEventArgs e) => await Task.Run(() => {
      this.NotifyClientIncomming(new DataEvent(Encoding.UTF8.GetString(e.Message), e.Topic, DateTime.Now));
    });

    public override void Send(String topic, String data) {
      this.client.Publish(topic, Encoding.UTF8.GetBytes(data));
      this.NotifyClientSending(new DataEvent(data, topic, DateTime.Now));
    }

    #region IDisposable Support
    private Boolean disposedValue = false;

    public override Boolean IsConnected {
      get {
        if(this.client != null) {
          return this.client.IsConnected;
        } else {
          return false;
        }
      }
    }

    protected virtual void Dispose(Boolean disposing) {
      if(!this.disposedValue) {
        if(disposing) {try {
            try {
              this.connectionWatcher.Abort();
              this.connectionWatcher = null;
            } catch { }
            this.Disconnect(true);
          } catch (Exception) { }
        }
        this.client = null;
        this.disposedValue = true;
      }
    }

    public override void Dispose() {
      this.Dispose(true);
      GC.SuppressFinalize(this);
    }
    #endregion
  }
}
