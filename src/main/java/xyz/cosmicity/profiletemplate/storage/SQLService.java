package xyz.cosmicity.profiletemplate.storage;

import co.aikar.idb.DatabaseOptions;
import co.aikar.idb.HikariPooledDatabase;
import co.aikar.idb.PooledDatabaseOptions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalNotification;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.jetbrains.annotations.NotNull;

import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class SQLService {

    private final SQLTable profileTable;

    @NonNull
    private final LoadingCache<@NotNull UUID, @NotNull Profile> profileCache;

    public SQLService(String host, String database, String username, String password) {

        DatabaseOptions options = DatabaseOptions.builder()
                .mysql(username,
                        password,
                        database,
                        host).build();

        SQLUtils.setDb(new HikariPooledDatabase(PooledDatabaseOptions.builder().options(options).build()));

        profileTable = new SQLTable("profiles", "uuid","VARCHAR(16)","joined TEXT discordid VARCHAR(32)");

        profileCache = CacheBuilder.newBuilder()
                .removalListener(this::saveProfile)
                .build(CacheLoader.from(this::loadProfile));
    }

    public void onDisable() {
        profileCache.invalidateAll();
        profileCache.cleanUp();
    }

    @NonNull
    private Profile loadProfile(@NotNull final UUID uuid) {
        Profile profile = new Profile(uuid);
        if(! SQLUtils.holdsKey(profileTable, "\""+uuid.toString()+"\"")) return profile;
        return profile.loadAttributes(profileTable);
    }

    private void saveProfile(@NotNull final RemovalNotification<@NotNull UUID, @NotNull Profile> notification) {
        notification.getValue().saveTo(profileTable);
    }

    /*
     * load or unload a profile from cache
     */
    public void validate(@NotNull final Profile profile) {
        this.profileCache.put(profile.getUuid(), profile);
    }
    public void invalidate(@NotNull final Profile profile) {
        this.profileCache.invalidate(profile.getUuid());
    }

    public Profile wrap(@NotNull final UUID uuid) {
        try {
            return profileCache.get(uuid);
        }
        catch(final ExecutionException e) {
            e.printStackTrace();
        }
        return null;
    }

    public Profile wrapIfLoaded(@NotNull final UUID uuid) {
        return profileCache.getIfPresent(uuid);
    }

}
