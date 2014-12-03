package com.streever.hadoop.hive.ip.honey;

/**
 * Created by dstreev on 12/3/14.
 */
public class IPConverter {

    static Long convertFromString(String ipAddress) {
        if (ipAddress != null) {
            String[] ipParts = ipAddress.split("\\.");
            if (ipParts.length != 4) {
                return 0l;
            } else {
                Integer parts[] = new Integer[ipParts.length];

                for (int i=0;i<ipParts.length;i++) {
                    parts[i] = Integer.parseInt(ipParts[i]);
                }

                return Long.parseLong(Long.toString(parts[0] << 24 | parts[1] << 16 | parts[2] << 8 | parts[3],2),2);

            }
        } else {
            return 0l;
        }
    }

    static String convertFromLong(Long ipAddress) {
        if (ipAddress != null) {

            // Get the octets
            Long[] octets = new Long[4];
            octets[3] = ipAddress & 255;
            ipAddress = ipAddress >> 8;
            octets[2] = ipAddress & 255;
            ipAddress = ipAddress >> 8;
            octets[1] = ipAddress & 255;
            ipAddress = ipAddress >> 8;
            octets[0] = ipAddress & 255;

            StringBuilder sb = new StringBuilder();
            sb.append(octets[0].toString()).append(".");
            sb.append(octets[1].toString()).append(".");
            sb.append(octets[2].toString()).append(".");
            sb.append(octets[3].toString());

            return sb.toString();
        } else {
            return "";
        }
    }

}
