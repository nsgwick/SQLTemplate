package xyz.cosmicity.profiletemplate.storage;

import java.time.Instant;
import java.util.List;
import java.util.Date;
import java.util.UUID;

public class Profile {

    private final UUID key;
    private Date firstJoined;
    private String discord;

    public Profile(final UUID uuid) {
        key = uuid;
        firstJoined = new Date();
        discord = "";
    }

    public UUID getUuid() {
        return key;
    }
    public Date getFirstJoined() {
        return firstJoined;
    }
    public void setDiscord(final String dId) {
        discord = dId;
    }
    public String getDiscord() {
        return discord;
    }

    public Profile loadAttributes(final SQLTable table) {
        List<Object> row = SQLUtils.getRow(table, "\""+key.toString()+"\"","joined","discordid");
        firstJoined = Date.from(Instant.ofEpochMilli(Long.parseLong((String) row.get(0))));
        return this;
    }

    public void saveTo(final SQLTable table) {
    }

}
