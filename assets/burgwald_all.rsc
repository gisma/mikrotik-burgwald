    :local ssid2 "Burgwald_01"
    :local psk2  "be45dhr0"
    :local ssid1 "GISMA-NET"
    :local psk1  "36171669"
    /interface bridge
    :local br [/interface bridge find where name="bridgeLocal"]
    :if ([:len $br] = 0) do={ add name=bridgeLocal }
    /interface bridge port
    :local bpE [/interface bridge port find where bridge="bridgeLocal" and interface="ether1"]
    :if ([:len $bpE] > 0) do={ :foreach i in=$bpE do={ remove $i } }
    add bridge=bridgeLocal interface=ether1
    :local bpW [/interface bridge port find where bridge="bridgeLocal" and interface="wlan1"]
    :if ([:len $bpW] > 0) do={ :foreach i in=$bpW do={ remove $i } }
    add bridge=bridgeLocal interface=wlan1
    /interface list
    :local wan [/interface list find where name="WAN"]
    :if ([:len $wan] = 0) do={ add name=WAN }
    :local lan [/interface list find where name="LAN"]
    :if ([:len $lan] = 0) do={ add name=LAN }
    /interface list member
    :if ([:len [/interface list member find where list=WAN and interface=wlan1]] = 0) do={ add list=WAN interface=wlan1 }
    :if ([:len [/interface list member find where list=LAN and interface=ether1]] = 0) do={ add list=LAN interface=ether1 }
    /interface wireless cap
    :do { set enabled=no } on-error={}
    /interface wireless set [find default-name=wlan1] band=2ghz-b/g/n wireless-protocol=802.11 mode=station-pseudobridge ssid="" frequency=auto disabled=no
    /interface wireless security-profiles
    :local sp1 ("sp-" . $ssid1)
    :if ([:len [find where name=$sp1]] = 0) do={ add name=$sp1 authentication-types=wpa-psk,wpa2-psk mode=dynamic-keys wpa-pre-shared-key=$psk1 wpa2-pre-shared-key=$psk1 } else={ set [find where name=$sp1] authentication-types=wpa-psk,wpa2-psk mode=dynamic-keys wpa-pre-shared-key=$psk1 wpa2-pre-shared-key=$psk1 }
    :local sp2 ("sp-" . $ssid2)
    :if ([:len [find where name=$sp2]] = 0) do={ add name=$sp2 authentication-types=wpa-psk,wpa2-psk mode=dynamic-keys wpa-pre-shared-key=$psk2 wpa2-pre-shared-key=$psk2 } else={ set [find where name=$sp2] authentication-types=wpa-psk,wpa2-psk mode=dynamic-keys wpa-pre-shared-key=$psk2 wpa2-pre-shared-key=$psk2 }
    /interface wireless connect-list
    :local cl1 [/interface wireless connect-list find where interface=wlan1 and ssid=$ssid1]
    :if ([:len $cl1] > 0) do={ :foreach i in=$cl1 do={ remove $i } }
    :local cl2 [/interface wireless connect-list find where interface=wlan1 and ssid=$ssid2]
    :if ([:len $cl2] > 0) do={ :foreach i in=$cl2 do={ remove $i } }
    add interface=wlan1 ssid=$ssid1 security-profile=$sp1 connect=yes priority=1
    add interface=wlan1 ssid=$ssid2 security-profile=$sp2 connect=yes priority=2
    /ip dhcp-client
    :if ([:len [find where interface="bridgeLocal"]] = 0) do={ add interface=bridgeLocal disabled=no add-default-route=yes use-peer-dns=yes use-peer-ntp=yes } else={ set [find where interface="bridgeLocal"] disabled=no add-default-route=yes use-peer-dns=yes use-peer-ntp=yes }
    /ip route
    :local sr [/ip route find where dst-address="0.0.0.0/0" and !dynamic]
    :if ([:len $sr] > 0) do={ :foreach r in=$sr do={ remove $r } }
    /ip dns set allow-remote-requests=yes servers=8.8.8.8,1.1.1.1
    /lora servers
    :if ([:len [find where name="TTN V3 (eu1)"]] = 0) do={ add name="TTN V3 (eu1)" address=eu1.cloud.thethings.network up-port=1700 down-port=1700 }
    /lora
    :local l [/lora find]
    :if ([:len $l] > 0) do={ disable 0; set 0 servers="TTN V3 (eu1)" channel-plan=eu-868 network=public forward=crc-valid,crc-error; enable 0 }
    /tool netwatch
    :local nw [/tool netwatch find where host="8.8.8.8"]
    :if ([:len $nw] = 0) do={ add host=8.8.8.8 interval=60s timeout=5s up-script="" down-script="/interface wireless disable wlan1; :delay 2s; /interface wireless enable wlan1" }
    /system clock set time-zone-name=Europe/Berlin
