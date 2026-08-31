// Add to app/build.gradle.kts

dependencies {
    implementation("net.java.dev.jna:jna:5.14.0@aar")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-android:1.8.1")
    implementation("androidx.work:work-runtime-ktx:2.9.0") // for periodic background sync
}

android {
    sourceSets["main"].jniLibs.srcDirs("src/main/jniLibs")
    // src/main/jniLibs/arm64-v8a/libsync_core.so
    // src/main/jniLibs/armeabi-v7a/libsync_core.so
    // src/main/jniLibs/x86_64/libsync_core.so
    // built via: cargo install cargo-ndk
    //            cargo ndk -t arm64-v8a -t armeabi-v7a -t x86_64 -o app/src/main/jniLibs build --release
}
