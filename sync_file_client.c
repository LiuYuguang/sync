#include <stdio.h>
#include <string.h>    //for memmove
#include <stdlib.h>    //for malloc realloc free
#include <fcntl.h>     //O_WRONLY O_CREAT
#include <errno.h>
#include <time.h>      //for gettimeofday localtime
#include <stdarg.h>    //for va_start vsnprintf va_end
#include <stddef.h>    //for size_t
#include <sys/types.h> //for ssize_t

#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

int write_log(const char* level, const char* func, const int line, const char *format, ...)
{
	int  len;
	int  retu;
	int  size;
	static char buf[4096];
	pid_t pid;

	pid = getpid();

	va_list arg;
	struct timeval tv;
	struct tm* tm_log;
	time_t time_log;

	gettimeofday(&tv, NULL);
	time_log = tv.tv_sec;
	tm_log = localtime(&time_log);

	size = sizeof(buf)-1;
	snprintf(buf, size, "%02d-%02d-%02d %02d:%02d:%02d.%06ld [%s] %s:%d %ld - ",
		tm_log->tm_year+1900,tm_log->tm_mon+1,tm_log->tm_mday, 
		tm_log->tm_hour, tm_log->tm_min, tm_log->tm_sec, tv.tv_usec, 
		level,func,line,
		(long)pid
	);
	len = strlen(buf);
	va_start(arg, format);
	vsnprintf(buf+len, size-len, format, arg);
	va_end(arg);
	len = strlen(buf);
	buf[len++] = '\n';
    return write(1,buf,len);
}

#if DEBUG
#define LOG_DEBUG(args...)\
    write_log("DEBUG",__FUNCTION__,__LINE__,##args);
#else
#define LOG_DEBUG(args...)
#endif

#define LOG_INFO(args...)\
	write_log("INFO",__FUNCTION__,__LINE__,##args);

#define LOG_ERROR(args...)\
	write_log("ERROR",__FUNCTION__,__LINE__,##args);

#define LOG_WARN(args...)\
	write_log("WARN",__FUNCTION__,__LINE__,##args);

#define LOG_FATAL(args...)\
	write_log("FATAL",__FUNCTION__,__LINE__,##args);

typedef struct{
    int fd;
    size_t offset;
    size_t len;
    size_t cap;
    void *buff;
}Cache;

#define CACHE_SIZE 4096
#define BUFF_SIZE 4096

Cache* cache_create(int fd){
    Cache *c = malloc(sizeof(Cache));
    if(c == NULL){
        return NULL;
    }

    c->fd = fd;
    c->offset = 0;
    c->len = 0;
    c->cap = CACHE_SIZE;
    c->buff = malloc(sizeof(unsigned char) * c->cap);
    return c;
}

void cache_destory(Cache* c){
    free(c->buff);
    free(c);
}

ssize_t cache_readline(Cache *c,void *buff,size_t buffSize){
    if(c == NULL || c->buff == NULL){
        return -1;
    }
    size_t index = c->offset;
    ssize_t len;
    for(;;){
        for(;index<c->len&&((unsigned char*)c->buff)[index]!='\n';index++){}
        if(index<c->len){
            index++;
            if((index-c->offset) > buffSize){
                errno = E2BIG;
                return -1;
            }
            buffSize = (index-c->offset);
            memmove(buff,c->buff+c->offset,buffSize);
            c->offset = index;
            return buffSize;
        }

        if(c->len == c->cap){
            if(c->offset == 0){
                c->cap<<=1;
                c->buff = realloc(c->buff,c->cap);
            }else{
                c->len -= c->offset;
                memmove(c->buff,c->buff+c->offset,c->len);     
                c->offset=0;
                index = c->len;
            }
        }

        len = read(c->fd,c->buff + c->len,c->cap - c->len);
        if(len <= 0){
            return -1;
        }
        c->len += len;
    }
    return -1;
}

ssize_t cache_readbytes(Cache *c,void *buff,size_t buffSize){
    if(c == NULL || c->buff == NULL){
        return -1;
    }
    size_t len;
    for(;;){
        if((c->len-c->offset) > buffSize){
            memmove(buff,c->buff+c->offset,buffSize);
            c->offset += buffSize;
            return buffSize;
        }

        if(c->len == c->cap){
            if(c->offset == 0){
                c->cap<<=1;
                c->buff = realloc(c->buff,c->cap);
            }else{
                c->len -= c->offset;
                memmove(c->buff,c->buff+c->offset,c->len);     
                c->offset=0;
            }
        }

        len = read(c->fd,c->buff + c->len,c->cap - c->len);
        if(len <= 0){
            return -1;
        }
        c->len += len;
    }
    return -1;
}

int main(int argc,char* argv[]){
    if(argc < 4){
        printf("sync_file_client host port name\nname must be unique for each client process\n");
        exit(0);
    }

    const char* host = argv[1];
    const short port = atoi(argv[2]);
    const char* name = argv[3];
    
    int sockfd;
    struct sockaddr_in addr;
    socklen_t addrSize = sizeof(addr);
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    inet_pton(AF_INET,host,&addr.sin_addr);

    sockfd = socket(AF_INET,SOCK_STREAM,0);

    if(connect(sockfd,(struct sockaddr*)&addr,addrSize) == -1){
        LOG_ERROR("connect fail %d, %s",errno,strerror(errno));
        exit(0);
    }

    unsigned char *buf = NULL;
    int bufLen,bufCap=BUFF_SIZE;
    buf = malloc(bufCap);

    Cache* cache = cache_create(sockfd);
    char file_name[128] = {0};
    char file_time_str[128] = {0};
    char file_size_str[128] = {0};
    size_t file_time,file_size;
    ssize_t ret;
    int fd;
    char *index;
    size_t index_len;

    //发送name，由server判断name对应的最后一笔数据
    bufLen = sprintf((char*)buf,"%s\r\n",name);
    bufLen = write(sockfd,buf,bufLen);
    if(bufLen == -1){
        LOG_ERROR("write fail %d, %s",errno,strerror(errno));
        exit(0);
    }

    for(;;){
        memset(file_name,0,sizeof(file_name));
        ret = cache_readline(cache,file_name,sizeof(file_name));
        if(ret <= 0){
            LOG_ERROR("ret%d, errno%d, %s",ret,errno,strerror(errno));
            break;
        }
        if(ret > 1 && file_name[ret-2] == '\r') file_name[ret-2] = '\x0';
        if(ret > 0 && file_name[ret-1] == '\n') file_name[ret-1] = '\x0';
        	
        index = strstr(file_name,"/tmp");
        if(index != NULL){
            index_len = strlen(index)+1;
        	memmove(file_name,index,index_len);
        	memmove(file_name+12,file_name,index_len);
        	memmove(file_name,"/home/lyg001",12);
        }

        memset(file_time_str,0,sizeof(file_time_str));
        ret = cache_readline(cache,file_time_str,sizeof(file_time_str));
        if(ret <= 0){
            LOG_ERROR("errno%d, %s",errno,strerror(errno));
            break;
        }
        if(ret > 1 && file_time_str[ret-2] == '\r') file_time_str[ret-2] = '\x0';
        if(ret > 0 && file_time_str[ret-1] == '\n') file_time_str[ret-1] = '\x0';
        file_time = atol(file_time_str);

        memset(file_size_str,0,sizeof(file_size_str));
        ret = cache_readline(cache,file_size_str,sizeof(file_size_str));
        if(ret <= 0){
            LOG_ERROR("errno%d, %s",errno,strerror(errno));
            break;
        }
        if(ret > 1 && file_size_str[ret-2] == '\r') file_size_str[ret-2] = '\x0';
        if(ret > 0 && file_size_str[ret-1] == '\n') file_size_str[ret-1] = '\x0';
        file_size = atol(file_size_str);

        //keepalive
        if(file_name[0]=='\x0' && file_time==0 && file_size==0){
            //空行
            ret = cache_readline(cache,buf,bufCap);
            if(ret <= 0){
                LOG_ERROR("errno%d, %s",errno,strerror(errno));
                break;
            }

            bufLen = write(sockfd,"\r\n\r\n",4);
            if(bufLen == -1){
                LOG_ERROR("errno%d, %s",errno,strerror(errno));
                break;
            }
            continue;
        }

        if(file_size > bufCap){
            while(file_size > bufCap){
                bufCap<<=1;
            }
            buf = realloc(buf,bufCap);
            if(buf == NULL){
                LOG_ERROR("errno%d, %s",errno,strerror(errno));
                break;
            }
        }
        
        memset(buf,0,bufCap);
        fd = open(file_name,O_WRONLY|O_CREAT|O_TRUNC,0664);
        if(fd == -1){
            LOG_ERROR("errno%d, %s",errno,strerror(errno));
            break;
        }

        if(file_size > 0){
            ret = cache_readbytes(cache,buf,file_size);
            if(ret <= 0){
                LOG_ERROR("errno%d, %s",errno,strerror(errno));
                break;
            }

            ret = write(fd,buf,file_size);
            if(ret <= 0){
                LOG_ERROR("errno%d, %s",errno,strerror(errno));
                break;
            }
        }
        close(fd);

        //空行
        ret = cache_readline(cache,buf,bufCap);
        if(ret <= 0){
            LOG_ERROR("errno%d, %s",errno,strerror(errno));
            break;
        }

        bufLen = sprintf((char*)buf,"%s\r\n%ld\r\n",file_name,file_time);
        bufLen = write(sockfd,buf,bufLen);
        if(bufLen == -1){
            LOG_ERROR("errno%d, %s",errno,strerror(errno));
            break;
        }

        LOG_INFO("file_name%s, file_time%ld, file_size%ld sync finish",file_name,file_time,file_size);

    }

    free(buf);
    cache_destory(cache);
    close(sockfd);
    return 0;
}