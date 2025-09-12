# sep/09/2025 18:58:37 by RouterOS 6.49.18
# software id = 1U0Z-B702
#
# model = RBwAPR-2nD
# serial number = HH60AA2PHPN
/interface bridge
add admin-mac=F4:1E:57:59:10:6C auto-mac=no comment=defconf name=bridgeLocal
/interface wireless
set [ find default-name=wlan1 ] band=2ghz-b/g/n disabled=no frequency=auto \
    mode=station-pseudobridge ssid=GISMA-NET wireless-protocol=802.11
/interface list
add name=WAN
add name=LAN
/interface wireless security-profiles
set [ find default=yes ] authentication-types=wpa-psk,wpa2-psk group-ciphers=\
    tkip,aes-ccm mode=dynamic-keys supplicant-identity=MikroTik \
    unicast-ciphers=tkip,aes-ccm wpa-pre-shared-key=36171669 \
    wpa2-pre-shared-key=36171669
/lora servers
add address=eu.mikrotik.thethings.industries down-port=1700 name=TTN-EU \
    up-port=1700
add address=us.mikrotik.thethings.industries down-port=1700 name=TTN-US \
    up-port=1700
add address=eu1.cloud.thethings.industries down-port=1700 name=\
    "TTS Cloud (eu1)" up-port=1700
add address=nam1.cloud.thethings.industries down-port=1700 name=\
    "TTS Cloud (nam1)" up-port=1700
add address=au1.cloud.thethings.industries down-port=1700 name=\
    "TTS Cloud (au1)" up-port=1700
add address=eu1.cloud.thethings.network down-port=1700 name="TTN V3 (eu1)" \
    up-port=1700
add address=nam1.cloud.thethings.network down-port=1700 name="TTN V3 (nam1)" \
    up-port=1700
add address=au1.cloud.thethings.network down-port=1700 name="TTN V3 (au1)" \
    up-port=1700
/interface bridge port
add bridge=bridgeLocal comment=defconf disabled=yes interface=ether1
add bridge=bridgeLocal interface=ether1
add bridge=bridgeLocal interface=wlan1
/interface list member
add interface=wlan1 list=WAN
add interface=ether1 list=LAN
/interface wireless cap
set bridge=bridgeLocal discovery-interfaces=bridgeLocal interfaces=wlan1
/ip dhcp-client
add disabled=no interface=bridgeLocal
/ip dns
set allow-remote-requests=yes servers=8.8.8.8,1.1.1.1
/ip route
add distance=1 gateway=192.168.8.1
add disabled=yes distance=1 gateway=192.168.8.1
add disabled=yes distance=1 gateway=192.168.8.1
/lora
set 0 antenna=uFL disabled=no servers="TTN V3 (eu1)"
/system clock
set time-zone-name=Europe/Berlin
