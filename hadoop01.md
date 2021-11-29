### Linux集群分发脚本命令

#### scp：可以实现服务器与服务器之间的数据拷贝

scp  -r       $pdir/$fname                    $user@host:$pdir/fname 

​       递归    要拷贝的文件路径/名称    目的地用户@主机：目的地路径/名称

例：scp  -r  test-scp/   lijiale@hadoop02:/opt/module/       此命令是将本机上的目标文件推送给目标主机

例：scp  -r  lijiale@hadoop02:/opt/module/test-scp  /opt/module/  此命令是将对方的目标目录下文件拉取到本机的目标地址下

当然scp命令也可以由第三方机器，比如hadoop02来使用，将hadoop01上的文件发送给hadoop03

#### rsync 远程同步工具，可以避免拷贝相同的数据所造成不必要的时间浪费

scp是把整个文件都拷贝过去，rsync是只对差异文件做更新

例：rsync -av test-rsync/  root@hadoop02:/opt/module/test-rsync   将本机上的目标文件夹中的内容同步到目标主机的目的地址（只同步差异化文件）

#### 集群分发shell脚本

~~~txt
#!/bin/bash

#1. 判断参数个数
if [ $# -lt 1 ]
then
    echo Not Enough Arguement!
    exit;
fi

#2. 遍历集群所有机器
for host in hadoop01 hadoop02 hadoop03
do
    echo ====================  $host  ====================
    #3. 遍历所有目录，挨个发送

    for file in $@
    do
        #4. 判断文件是否存在
        if [ -e $file ]
            then
                #5. 获取父目录
                pdir=$(cd -P $(dirname $file); pwd)

                #6. 获取当前文件的名称
                fname=$(basename $file)
                ssh $host "mkdir -p $pdir"
                rsync -av $pdir/$fname $host:$pdir
            else
                echo $file does not exists!
        fi
    done
done
~~~

chmod 777 xsync



   