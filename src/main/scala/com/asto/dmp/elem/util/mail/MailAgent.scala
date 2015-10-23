package com.asto.dmp.elem.util.mail

import java.util.{Date, Properties}
import javax.mail._
import javax.mail.internet._

object MailAgent {
  def apply(mail:Mail) = new MailAgent(mail)
  def apply(context: String, subject: String = Mail.getPropByKey("mail_subject"), to: String = Mail.getPropByKey("mail_to")) =
    new MailAgent(new Mail(context, subject, to))
  def apply(t: Throwable, subject: String, to: String) = new MailAgent(new Mail(t, subject, to))
  def apply(t: Throwable, subject: String) = new MailAgent(new Mail(t, subject))
  def apply(t: Throwable) = new MailAgent(new Mail(t))
}

class MailAgent(mail: Mail) {
  val props = new Properties()
  props.put("mail.smtp.auth", "true")
  val session = Session.getDefaultInstance(props)
  val message = new MimeMessage(session)

  def sendMessage() = {
    if(Mail.prop.getProperty("mail_enable") == "true") {
      println("******************************** 邮件信息：" + mail + "********************************")
      message.setFrom(new InternetAddress(mail.from))
      setToCcBccRecipients()
      message.setSentDate(new Date())
      message.setSubject(mail.subject)
      message.setText(mail.context,"UTF-8")
      val transport = session.getTransport("smtp")
      transport.connect(mail.smtpHost, mail.from, mail.password)
      transport.sendMessage(message, message.getAllRecipients)
    }
  }

  // throws AddressException, MessagingException
  private def setToCcBccRecipients() {
    setMessageRecipients(mail.to, Message.RecipientType.TO)
    if (mail.cc != null && "".ne(mail.cc)) {
      setMessageRecipients(mail.cc, Message.RecipientType.CC)
    }
    if (mail.bcc != null && "".ne(mail.bcc)) {
      setMessageRecipients(mail.bcc, Message.RecipientType.BCC)
    }
  }

  // throws AddressException, MessagingException
  private def setMessageRecipients(recipient: String, recipientType: Message.RecipientType) {
    // had to do the asInstanceOf[...] call here to make scala happy
    val addressArray = buildInternetAddressArray(recipient).asInstanceOf[Array[Address]]
    if ((addressArray != null) && (addressArray.length > 0)) {
      message.setRecipients(recipientType, addressArray)
    }
  }

  // throws AddressException
  private def buildInternetAddressArray(address: String): Array[InternetAddress] = {
    // could test for a null or blank String but I'm letting parse just throw an exception
    InternetAddress.parse(address)
  }
}

