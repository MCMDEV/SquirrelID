package org.enginehub.squirrelid.cache;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import org.bson.BsonBinary;
import org.bson.BsonReader;
import org.bson.BsonWriter;
import org.bson.UuidRepresentation;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.configuration.CodecRegistries;
import org.enginehub.squirrelid.Profile;

import java.util.UUID;

public class MongoDBCache extends AbstractProfileCache {

    private final MongoCollection<Profile> profileCollection;

    public MongoDBCache(MongoCollection<Profile> profileCollection) {
        this.profileCollection = profileCollection.withCodecRegistry(
                CodecRegistries.fromCodecs(
                        new ProfileCodec()
                )
        );
    }

    @Override
    public void putAll(Iterable<Profile> profiles) {
        profileCollection.insertMany(ImmutableList.copyOf(profiles));
    }

    @Override
    public ImmutableMap<UUID, Profile> getAllPresent(Iterable<UUID> ids) {
        return profileCollection.find(Filters.in("uuid", ids)).into(ImmutableList.of()).stream()
                .collect(ImmutableMap.toImmutableMap(Profile::getUniqueId, profile -> profile));
    }

    private static class ProfileCodec implements Codec<Profile> {

        @Override
        public Profile decode(BsonReader reader, DecoderContext decoderContext) {
            UUID uuid = reader.readBinaryData("uuid").asUuid(UuidRepresentation.STANDARD);
            String name = reader.readString("name");
            return new Profile(uuid, name);
        }

        @Override
        public void encode(BsonWriter writer, Profile value, EncoderContext encoderContext) {
            writer.writeBinaryData("uuid", new BsonBinary(value.getUniqueId(), UuidRepresentation.STANDARD));
            writer.writeString("name", value.getName());
        }

        @Override
        public Class<Profile> getEncoderClass() {
            return Profile.class;
        }
    }
}
