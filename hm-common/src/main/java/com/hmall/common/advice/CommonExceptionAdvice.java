package com.hmall.common.advice;

import com.hmall.common.domain.R;
import com.hmall.common.exception.BadRequestException;
import com.hmall.common.exception.CommonException;
import com.hmall.common.exception.DbException;
import com.hmall.common.utils.WebUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.ObjectError;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.util.NestedServletException;

import java.net.BindException;
import java.util.stream.Collectors;

@RestControllerAdvice//这个注解告诉 Spring Boot：“这是一个全局异常拦截器，随时监听系统里抛出的任何异常。”
@Slf4j
public class CommonExceptionAdvice {

    @ExceptionHandler(DbException.class)//你可以看到有很多被这个注解修饰的方法。它相当于分类处理
    public Object handleDbException(DbException e) {
        log.error("mysql数据库操作异常 -> ", e);
        return processResponse(e);
    }

    @ExceptionHandler(CommonException.class)
    public Object handleBadRequestException(CommonException e) {
        log.error("自定义异常 -> {} , 异常原因：{}  ",e.getClass().getName(), e.getMessage());
        log.debug("", e);
        return processResponse(e);
    }

    @ExceptionHandler(MethodArgumentNotValidException.class)//(通常是实体类校验如 @Valid 失败)，它会把具体的字段错误提取出来并返回给前端。
    public Object handleMethodArgumentNotValidException(MethodArgumentNotValidException e) {
        String msg = e.getBindingResult().getAllErrors()
                .stream().map(ObjectError::getDefaultMessage)
                .collect(Collectors.joining("|"));
        log.error("请求参数校验异常 -> {}", msg);
        log.debug("", e);
        return processResponse(new BadRequestException(msg));
    }
    @ExceptionHandler(BindException.class)
    public Object handleBindException(BindException e) {
        log.error("请求参数绑定异常 ->BindException， {}", e.getMessage());
        log.debug("", e);
        return processResponse(new BadRequestException("请求参数格式错误"));
    }

    @ExceptionHandler(NestedServletException.class)
    public Object handleNestedServletException(NestedServletException e) {
        log.error("参数异常 -> NestedServletException，{}", e.getMessage());
        log.debug("", e);
        return processResponse(new BadRequestException("请求参数处理异常"));
    }

    @ExceptionHandler(Exception.class)
    public Object handleRuntimeException(Exception e) {
        log.error("其他异常 uri : {} -> ", WebUtils.getRequest().getRequestURI(), e);
        return processResponse(new CommonException("服务器内部异常", 500));
    }

    private ResponseEntity<R<Void>> processResponse(CommonException e){//将所有捕获到的异常，统一转换成前端能够解析的标准响应格式R对象
        return ResponseEntity.status(e.getCode()).body(R.error(e));
    }
}
