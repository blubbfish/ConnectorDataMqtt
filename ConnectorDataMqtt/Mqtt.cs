using System;
using System.Collections.Generic;
using System.Text;
using BlubbFish.Utils.IoT.Events;
using uPLibrary.Networking.M2Mqtt;
using uPLibrary.Networking.M2Mqtt.Messages;

namespace BlubbFish.Utils.IoT.Connector.Data {
  public class Mqtt : ADataBackend, IDisposable {
    private MqttClient client;

    public Mqtt(Dictionary<String, String> settings) : base(settings) {
      Int32 port = 1883;
      if(this.settings.ContainsKey("port")) {
        port = Int32.Parse(this.settings["port"]);
      }
      this.client = new MqttClient(this.settings["server"], port, false, null, null, MqttSslProtocols.None);
      Connect();
    }

    private void Connect() {
      this.client.MqttMsgPublishReceived += this.Client_MqttMsgPublishReceived;
      if (this.settings.ContainsKey("user") && this.settings.ContainsKey("pass")) {
        this.client.Connect(Guid.NewGuid().ToString(), this.settings["user"], this.settings["pass"]);
      } else {
        this.client.Connect(Guid.NewGuid().ToString());
      }
      if (this.settings.ContainsKey("topic")) {
        Int32 l = this.settings["topic"].Split(';').Length;
        Byte[] qos = new Byte[l];
        for (Int32 i = 0; i < qos.Length; i++) {
          qos[i] = MqttMsgBase.QOS_LEVEL_AT_LEAST_ONCE;
        }
        this.client.Subscribe(this.settings["topic"].Split(';'), qos);
      } else {
        this.client.Subscribe(new String[] { "#" }, new Byte[] { MqttMsgBase.QOS_LEVEL_AT_LEAST_ONCE });
      }
    }

    private void Client_MqttMsgPublishReceived(Object sender, MqttMsgPublishEventArgs e) {
      this.NotifyClientIncomming(new DataEvent(Encoding.UTF8.GetString(e.Message), e.Topic, DateTime.Now));
    }

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
            this.client.MqttMsgPublishReceived -= this.Client_MqttMsgPublishReceived;
            this.client.Unsubscribe(new String[] { "#" });
            this.client.Disconnect();
          } catch (Exception) { }
        }

        this.client = null;

        this.disposedValue = true;
      }
    }

    public override void Dispose() {
      Dispose(true);
      GC.SuppressFinalize(this);
    }
    #endregion
  }
}
