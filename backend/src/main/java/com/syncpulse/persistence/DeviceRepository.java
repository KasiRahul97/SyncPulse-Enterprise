package com.syncpulse.persistence;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Row;
import com.syncpulse.config.AppProperties;
import com.syncpulse.domain.Device;
import com.syncpulse.domain.DeviceType;
import jakarta.annotation.PostConstruct;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/** Data access for {@code device_registry} — all devices for a user live in one partition. */
@Repository
@DependsOn("schemaInitializer")
public class DeviceRepository {

    private final CqlSession session;
    private final String keyspace;

    private PreparedStatement upsertStmt;
    private PreparedStatement listStmt;
    private PreparedStatement touchStmt;

    public DeviceRepository(CqlSession session, AppProperties props) {
        this.session = session;
        this.keyspace = props.getCassandra().getKeyspace();
    }

    @PostConstruct
    void prepare() {
        upsertStmt = session.prepare("INSERT INTO " + keyspace + ".device_registry " +
                "(user_id, device_id, device_type, registered_at, last_seen_at) VALUES (?,?,?,?,?)");
        listStmt = session.prepare("SELECT * FROM " + keyspace + ".device_registry WHERE user_id=?");
        touchStmt = session.prepare("UPDATE " + keyspace + ".device_registry SET last_seen_at=? " +
                "WHERE user_id=? AND device_id=?");
    }

    public void register(Device d) {
        session.execute(upsertStmt.bind(
                d.userId(), d.deviceId(), d.deviceType().name(), d.registeredAt(), d.lastSeenAt()));
    }

    public void touch(String userId, String deviceId, Instant when) {
        session.execute(touchStmt.bind(when, userId, deviceId));
    }

    public List<Device> listForUser(String userId) {
        List<Device> result = new ArrayList<>();
        for (Row row : session.execute(listStmt.bind(userId))) {
            result.add(new Device(
                    row.getString("user_id"),
                    row.getString("device_id"),
                    DeviceType.valueOf(row.getString("device_type")),
                    row.getInstant("registered_at"),
                    row.getInstant("last_seen_at")));
        }
        return result;
    }
}
