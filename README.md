# kip

Encrypted backups using [XChaCha20Poly1305](https://tools.ietf.org/html/rfc7539) AEAD with a 24-byte nonce generated by
OS calls. The key used is a 32-byte SHA3_256 hash of a user provided secret.
That secret is then stored utilizing [argon2id](https://en.wikipedia.org/wiki/Argon2), a hybrid version of the key
derivation functions: argon2i & argon2d. This is used for verifying any changes to
your backup jobs and for the ability to auto-upload backups on increment (i.e.
every hour). Compression is avoided due to potential [CRIME](https://en.wikipedia.org/wiki/CRIME) & [BREACH](https://en.wikipedia.org/wiki/BREACH) attacks. [FastCDC](https://www.usenix.org/system/files/conference/atc16/atc16-paper-xia.pdf) is implemented for deduplication.

kip is the result of a personal challenge. Firstly, learn Rust. Secondly,
build myself a replacement encrypted backup tool because the one I'm currently using has
an expiring trial, which ends soon. It's closed source as well so I 
really don't know if the implementation of their encryption suite is flawed or not.
Plus, I really wanted to look into using XChaCha20Poly1305 which is not usually
supported by your typical backup tool. There's plenty of other open source encrypted 
backup options available, but this seemed like more fun for me.

## TODO

- Fix assembling multi-chunk files on restore
- Create sync_channel for 10 concurrent file uploads during `run.upload()`
- Implement SQLite or similar file DB for jobs storage
- Implement exclusions list
- Add Google Drive as a provider
- Add Dropbox as a provider
- Add local path/network path as a provider

## Usage

#### Create a new backup job:

```bash
$ kip init <job>
$ kip init documents_backup
$ kip init profile_backup
```

#### Remove a backup job:

```bash
$ kip remove <job>
$ kip rm documents_backup
```

#### Add files to a backup job:

```bash
$ kip add <job> -f <files>
$ kip add documents_backup -f "Documents/"
$ kip add profile_backup -f "Documents/" "Desktop/" "Downloads/" ".bashrc"
```

#### Remove files from a backup job:

```bash
$ kip remove <job> -f <files>
$ kip rm profile_backup -f ".bashrc" "Desktop/"
```

#### Start a manual backup run:

```bash
$ kip push <job>
$ kip push documents_backup
```

#### Start a restore:

```bash
$ kip pull <job> -r <run>
$ kip pull documents_backup -r 1
```

#### List backup jobs with their metadata:

```bash
$ kip list <job> -r <run>
$ kip ls
$ kip ls documents_backup
$ kip ls documents_backup -r 1
```