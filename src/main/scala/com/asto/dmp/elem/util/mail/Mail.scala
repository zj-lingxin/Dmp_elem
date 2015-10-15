package com.asto.dmp.elem.util.mail

import java.io.FileInputStream
import java.util.Properties
import com.asto.dmp.elem.base.Constants

object Mail {
  private val prop = new Properties()
  /*
  在spark-submit中加入--driver-java-options -DPropPath=/home/hadoop/prop.properties的参数后，
  使用System.getProperty("PropPath")就能获取路径：/home/hadoop/prop.properties
   */
  private val propPath = System.getProperty("PropPath")
  private val hasPropPath = if (propPath == null) false else true

  //如果spark-submit中指定了prop.properties文件的路径，那么使用prop.properties中的属性，否则使用该类中定义的属性
  if (hasPropPath) {
    prop.load(new FileInputStream(propPath))
  } else {
    prop.put("mail_to", Constants.Mail.TO)
    prop.put("mail_from", Constants.Mail.FROM)
    prop.put("mail_password", Constants.Mail.PASSWORD)
    prop.put("mail_smtpHost", Constants.Mail.SMTPHOST)
    prop.put("mail_subject", Constants.Mail.SUBJECT)
    prop.put("mail_cc", Constants.Mail.CC)
    prop.put("mail_bcc", Constants.Mail.BCC)
    prop.put("mail_to_fraud", Constants.Mail.TO_FRAUD)
    prop.put("mail_to_score", Constants.Mail.TO_SCORE)
    prop.put("mail_to_access", Constants.Mail.TO_ACCESS)
    prop.put("mail_to_credit", Constants.Mail.TO_CREDIT)
    prop.put("mail_to_loan_after", Constants.Mail.TO_LOAN_AFTER)
  }

  def getPropByKey(propertyKey: String): String = {
    if (hasPropPath)
      new String(prop.getProperty(propertyKey).getBytes("ISO-8859-1"), "utf-8")
    else
      prop.getProperty(propertyKey)
  }
}

class Mail(var privateContext: String, var subject: String = Mail.getPropByKey("mail_subject"), var to: String = Mail.getPropByKey("mail_to")) {
  //发件箱
  var from = Mail.prop.getProperty("mail_from")
  //发件箱的密码
  var password = Mail.prop.getProperty("mail_password")
  //简单邮件传送协议服务器
  var smtpHost = Mail.prop.getProperty("mail_smtpHost")
  //抄送给哪些邮箱，多个邮箱之前用“，”分隔
  var cc = Mail.prop.getProperty("mail_cc")
  //密送给哪些邮箱，多个邮箱之前用“，”分隔
  var bcc = Mail.prop.getProperty("mail_bcc")

  def this(t: Throwable) {
    this("")
    context_=(t)
  }

  def this(t: Throwable, subject: String) {
    this(t)
    this.subject = subject
  }

  def this(t: Throwable, subject: String, to: String) {
    this(t, subject)
    this.to = to
  }

  def context_=(t: Throwable) {
    this.privateContext = t + "\n" + t.getStackTraceString
  }

  def context_=(context: String) {
    this.privateContext = context
  }

  def context: String = {
    this.privateContext
  }

  def setTo(to: String): this.type = {
    this.to = to
    this
  }

  def setFrom(from: String): this.type = {
    this.from = from
    this
  }

  def setPassword(password: String): this.type = {
    this.password = password
    this
  }

  def setSmptHost(smtpHost: String): this.type = {
    this.smtpHost = smtpHost
    this
  }

  def setSubject(subject: String): this.type = {
    this.subject = subject
    this
  }

  def setContext(context: String): this.type = {
    this.privateContext = context
    this
  }

  def setContext(t: Throwable): this.type = {
    context_=(t)
    this
  }

  def setCc(cc: String): this.type = {
    this.cc = cc
    this
  }

  def setBcc(bcc: String): this.type = {
    this.bcc = bcc
    this
  }

  override def toString: String = {
    s"\nto:$to\nsubject:$subject\nfrom:$from\npassword:$password \nsmtpHost:$smtpHost\ncc:$cc\nbcc:$bcc \n"
  }
}

