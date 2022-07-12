"use strict";

const mqtt = require("async-mqtt");
const manager = require("./manager");
const { LockedStatus, sleep, AudioManage } = require("ttlock-sdk-js");

class HomeAssistant {
  /**
   *
   * @param {import('./manager')} manager
   * @param {Object} options
   * @param {string} options.mqttUrl
   * @param {string} options.mqttUser
   * @param {string} options.mqttPass
   * @param {string} options.discovery_prefix
   */
  constructor(options) {
    this.mqttUrl = options.mqttUrl;
    this.mqttUser = options.mqttUser;
    this.mqttPass = options.mqttPass;
    this.discovery_prefix = options.discovery_prefix || "homeassistant";
    this.configuredLocks = new Set();

    this.connected = false;

    manager.on("lockPaired", this._onLockPaired.bind(this));
    manager.on("lockConnected", this._onLockConnected.bind(this));
    manager.on("lockUnlock", this._onLockUnlock.bind(this));
    manager.on("lockLock", this._onLockLock.bind(this));
    manager.on("lockBatteryUpdated", this._onLockBatteryUpdated.bind(this));
  }

  async connect() {
    if (!this.connected) {
      this.client = await mqtt.connectAsync(this.mqttUrl, {
        username: this.mqttUser,
        password: this.mqttPass,
      });
      this.client.on("message", this._onMQTTMessage.bind(this));
      await this.client.subscribe("ttlock/+/set");
      this.connected = true;
      console.log("MQTT connected");
    }
  }

  /**
   * Construct a unique ID for a lock, based on the MAC address
   * @param {import('ttlock-sdk-js').TTLock} lock
   */
  getLockId(lock) {
    const address = lock.getAddress();
    return address.split(":").join("").toLowerCase();
  }

  /**
   * Configure a lock device in HA
   * @param {import('ttlock-sdk-js').TTLock} lock
   */
  async configureLock(lock) {
    if (this.connected && !this.configuredLocks.has(lock.getAddress())) {
      // setup lock entity
      const id = this.getLockId(lock);
      const name = lock.getName();
      const device = {
        identifiers: ["ttlock_" + id],
        name: name,
        manufacturer: lock.getManufacturer(),
        model: lock.getModel(),
        sw_version: lock.getFirmware(),
      };

      // setup lock state
      const configLockTopic =
        this.discovery_prefix + "/lock/" + id + "/lock/config";
      const lockPayload = {
        unique_id: "ttlock_" + id,
        name: name,
        device: device,
        state_topic: "ttlock/" + id,
        command_topic: "ttlock/" + id + "/set",
        payload_lock: "LOCK",
        payload_unlock: "UNLOCK",
        state_locked: "LOCK",
        state_unlocked: "UNLOCK",
        value_template: "{{ value_json.state }}",
        optimistic: false,
        retain: false,
      };
      if (process.env.MQTT_DEBUG == "1") {
        console.log(
          "MQTT Publish",
          configLockTopic,
          JSON.stringify(lockPayload)
        );
      }
      let res = await this.client.publish(
        configLockTopic,
        JSON.stringify(lockPayload),
        { retain: true }
      );

      // setup battery sensor
      const configBatteryTopic =
        this.discovery_prefix + "/sensor/" + id + "/battery/config";
      const batteryPayload = {
        unique_id: "ttlock_" + id + "_battery",
        name: name + " Battery",
        device: device,
        device_class: "battery",
        unit_of_measurement: "%",
        state_topic: "ttlock/" + id,
        value_template: "{{ value_json.battery }}",
      };
      if (process.env.MQTT_DEBUG == "1") {
        console.log(
          "MQTT Publish",
          configBatteryTopic,
          JSON.stringify(batteryPayload)
        );
      }
      res = await this.client.publish(
        configBatteryTopic,
        JSON.stringify(batteryPayload),
        { retain: true }
      );

      // setup rssi sensor
      const configRssiTopic =
        this.discovery_prefix + "/sensor/" + id + "/rssi/config";
      const rssiPayload = {
        unique_id: "ttlock_" + id + "_rssi",
        name: name + " RSSI",
        device: device,
        device_class: "signal_strength",
        unit_of_measurement: "dB",
        state_topic: "ttlock/" + id,
        value_template: "{{ value_json.rssi }}",
      };
      if (process.env.MQTT_DEBUG == "1") {
        console.log(
          "MQTT Publish",
          configRssiTopic,
          JSON.stringify(rssiPayload)
        );
      }
      res = await this.client.publish(
        configRssiTopic,
        JSON.stringify(rssiPayload),
        { retain: true }
      );

      if (lock.hasAutoLock()) {
        // setup autolock control 
        const configAutoLockTopic =
          this.discovery_prefix + "/number/" + id + "/autolock/config";
        const autoLockPayload = {
          unique_id: "ttlock_" + id + "_autolock",
          name: name + " AutoLock Time",
          device: device,
          unit_of_measurement: "s",
          icon: "mdi:clock",
          state_topic: "ttlock/" + id,
          value_template: "{{ value_json.autolock }}",
          min: 0,
          max: 60,
          command_topic: "ttlock/" + id + "/set",
          command_template: "AUTOLOCK {{ value }}",
        };
        if (process.env.MQTT_DEBUG == "1") {
          console.log(
            "MQTT Publish",
            configAutoLockTopic,
            JSON.stringify(autoLockPayload)
          );
        }
        res = await this.client.publish(
          configAutoLockTopic,
          JSON.stringify(autoLockPayload),
          { retain: true }
        );
      }

      if (lock.hasLockSound()) {
        // setup audio control 
        const configAudioTopic =
          this.discovery_prefix + "/switch/" + id + "/audio/config";
        const audioPayload = {
          unique_id: "ttlock_" + id + "_audio",
          name: name + " Audio",
          device: device,
          icon: "mdi:speaker",
          state_topic: "ttlock/" + id,
          value_template: "{{ value_json.audio }}",
          command_topic: "ttlock/" + id + "/set",
          payload_on: "AUDIO ON",
          payload_off: "AUDIO OFF"
        };
        if (process.env.MQTT_DEBUG == "1") {
          console.log(
            "MQTT Publish",
            configAudioTopic,
            JSON.stringify(audioPayload)
          );
        }
        res = await this.client.publish(
          configAudioTopic,
          JSON.stringify(audioPayload),
          { retain: true }
        );
      }

      this.configuredLocks.add(lock.getAddress());
    }
  }

  /**
   * Update the readings of a lock in HA
   * @param {import('ttlock-sdk-js').TTLock} lock
   */
  async updateLockState(lock) {
    if (this.connected) {
      const id = this.getLockId(lock);
      const stateTopic = "ttlock/" + id;
      const lockedStatus = await lock.getLockStatus();
      let statePayload = {
        battery: lock.getBattery(),
        rssi: lock.getRssi(),
      };
      if (lockedStatus != LockedStatus.UNKNOWN) {
        statePayload.state =
          lockedStatus == LockedStatus.LOCKED ? "LOCK" : "UNLOCK";
      }

      if (lock.hasLockSound()) {
        statePayload.audio = (await lock.getLockSound()) == AudioManage.TURN_ON ? true : false;
      }

      if (lock.hasAutoLock()) {
        statePayload.autolock = await lock.getAutolockTime();
      }

      if (process.env.MQTT_DEBUG == "1") {
        console.log("MQTT Publish", stateTopic, JSON.stringify(statePayload));
      }
      const res = await this.client.publish(
        stateTopic,
        JSON.stringify(statePayload),
        { retain: true }
      );
    }
  }

  /**
   *
   * @param {import('ttlock-sdk-js').TTLock} lock
   */
  async _onLockPaired(lock) {
    await this.configureLock(lock);
  }

  /**
   *
   * @param {import('ttlock-sdk-js').TTLock} lock
   */
  async _onLockConnected(lock) {
    await this.configureLock(lock);
    await this.updateLockState(lock);
  }

  /**
   *
   * @param {import('ttlock-sdk-js').TTLock} lock
   */
  async _onLockUnlock(lock) {
    await this.updateLockState(lock);
  }

  /**
   *
   * @param {import('ttlock-sdk-js').TTLock} lock
   */
  async _onLockLock(lock) {
    await this.updateLockState(lock);
  }

  /**
   * @param {import('ttlock-sdk-js').TTLock} lock
   */
  async _onLockBatteryUpdated(lock) {
    await this.updateLockState(lock);
  }

  /**
   *
   * @param {string} topic
   * @param {Buffer} message
   */
  async _onMQTTMessage(topic, message) {
    /**
     * Topic: ttlock/e1581b3a605e/set
       Message: UNLOCK
     */
    let topicArr = topic.split("/");
    if (
      topicArr.length == 3 &&
      topicArr[0] == "ttlock" &&
      topicArr[2] == "set" &&
      topicArr[1].length == 12
    ) {
      let address = "";
      for (let i = 0; i < topicArr[1].length; i++) {
        address += topicArr[1][i];
        if (i < topicArr[1].length - 1 && i % 2 == 1) {
          address += ":";
        }
      }
      address = address.toUpperCase();
      const command = message.toString("utf8");
      if (process.env.MQTT_DEBUG == "1") {
        console.log("MQTT command:", address, command);
      }
      let result = false;
      const command_arr = command.split(" ");
      let tries = 0;
      while (!result && tries < 10) {
        tries += 1;
        switch (command_arr[0]) {
          case "LOCK":
            result = await manager.lockLock(address);
            break;
          case "UNLOCK":
            result = await manager.unlockLock(address);
            break;
          case "AUTOLOCK":
            if (command_arr[1] !== undefined) {
              const duration = parseInt(command.split(" ")[1]);
              result = await manager.setAutoLock(address, duration);
            } else {
              result = true;
            }
            break;
          case "AUDIO":
            if (command_arr[1] !== undefined) {
              const audio = command_arr[1] == "ON" ? true : false;
              result = await manager.setAudio(address, audio);
            } else {
              result = true;
            }
            break;
          default:
            result = true;
            break;
        }
        await sleep(1000);
      }
    } else if (process.env.MQTT_DEBUG == "1") {
      console.log("Topic:", topic);
      console.log("Message:", message.toString("utf8"));
    }
  }
}

module.exports = HomeAssistant;
