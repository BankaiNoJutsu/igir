DAT?
Hasheous offline lookup? Import json's to DB? Update mechanism?
Hasheous online lookup?
IGDB cache lookup?
IGDB online lookup?

Import DAT's to DB?

DB tables and columns needed?

Myrient search/lookup?

Renamer?

merger

chd converted

https://github.com/lostb1t/romsearch

todo:
- similarly to hash-threads, scan-threads
- remove sha256, not needed, not present in DATs

Efficient (avoid unnecessary, combine), Progress bar every step, multi threaded every step.

function 'scan_7z_entries', how to handle if 7za not available? We cannot skip

cargo run --bin igir -- copy -i '\\192.168.1.66\games\CONSOLE\roms\gba\' -o '\\192.168.1.66\games\TEST\DST\{romm}' -d 'D:\igir\dat\*' --filter-region EUR,USA,WORLD --filter-language EN,FR --enable-hasheous --igdb-client-id rl0merj5dddambnc9zoecpr16io2y5 --igdb-client-secret sk2c1h5ehmgokydah4m3tjereizveu --save-igdb-creds --hash-threads 32 --scan-threads 32 --cache-only --diag -vv

---

Let's show a progress per file when doing actions on files. Use multiprogress bars in order to achieve this. For every thread, we should see a progress bar for the current file in that thread

https://github.com/console-rs/indicatif/blob/main/examples/multi.rs
https://docs.rs/indicatif/latest/indicatif/struct.MultiProgress.html

---

cargo run --bin igir -- copy -i '\\192.168.1.66\games\CONSOLE\roms\gba\' -o '\\192.168.1.66\games\TEST\DST\{romm}' -d 'D:\igir\dat\*' --filter-region EUR,USA,WORLD --filter-language EN,FR --enable-hasheous --igdb-client-id rl0merj5dddambnc9zoecpr16io2y5 --igdb-client-secret sk2c1h5ehmgokydah4m3tjereizveu --save-igdb-creds --hash-threads 16 --scan-threads 16 --igdb-mode best-effort --diag -v`