### Linux集群分发脚本命令

scp：可以实现服务器与服务器之间的数据拷贝

scp  -r       $pdir/$fname                    $user@host:$pdir/fname 

​       递归    要拷贝的文件路径/名称    目的地用户@主机：目的地路径/名称

例：scp  -r  test-scp/   lijiale@hadoop02:/opt/module/       此命令是将本机上的目标文件推送给目标主机

例：scp  -r  lijiale@hadoop02:/opt/module/test-scp  /opt/module/  此命令是将对方的目标目录下文件拉取到本机的目标地址下

当然scp命令也可以由第三方机器，比如hadoop02来使用，将hadoop01上的文件发送给hadoop03



   