package com.netflix.migration.utils

import com.amazonaws.regions.Regions
import com.amazonaws.services.simpleemail.{AmazonSimpleEmailService, AmazonSimpleEmailServiceClientBuilder}
import com.amazonaws.services.simpleemail.model._
import com.netflix.migration.utils.Utils.migrationConf

import scala.collection.JavaConverters.setAsJavaSetConverter

object EmailSender {

//  val log = Logger.apply(this.getClass)
  val region = "us-west-2"

  val client: AmazonSimpleEmailService = AmazonSimpleEmailServiceClientBuilder
    .standard()
    .withRegion(Regions.fromName(region))
    .build()

  sealed trait EmailBodyType
  case object Text extends EmailBodyType
  case object Html extends EmailBodyType

  /**
   * Send an email to the specified recipients
   *
   * @param to
   *   set of recipients' email addresses
   * @param subject
   *   subject of the email
   * @param body
   *   body of the email
   * @param bodyType
   *   type of the email body
   */
  def send(
      to: Set[String],
      subject: String,
      body: String = "",
      bodyType: EmailBodyType = Html): Unit = {
    //    log.info(s"Sending an email to $to with subject $subject")
    var request = new SendEmailRequest()
      .withDestination(new Destination()
        .withToAddresses(to.asJava))
      .withMessage(
        new Message()
          .withBody(getBody(bodyType, body))
          .withSubject(new Content()
            .withCharset("UTF-8")
            .withData(s"do-not-reply: $subject")))
      .withSourceArn(MigrationConsts.AWS_SES_SOURCE_ARN)
    request = request.withSource(MigrationConsts.COMMUNICATION_EMAIL_ADDRESS)
    client.sendEmail(request)
  }

  /**
   * Convert plain text email body to HTML email body
   *
   * @param emailText
   *   plain text email body that includes newlines and list items
   * @return
   *   HTML formatted email body with proper line breaks and list items
   */
  def textToHtmlEmailBody(emailText: String): String = {
    val emailHtml =
      emailText
        .replaceAll("\\*", "<li>")
        .replaceAll("[.] \n", "</li>")
        .replaceAll("\n\n", "<br><br>")
        .replaceAll("\n", "<br>")

    s"""
    <html>
    <head></head>
    <body>
      <p>
        $emailHtml
      </p>
    </body>
    </html>"""
  }

  /**
   * Get the body of the email based on the body type
   *
   * @param bodyType
   *   type of the email body
   * @param body
   *   the text or html content of the email body
   * @return
   *   the email body in the specified format
   */
  def getBody(bodyType: EmailBodyType, body: String): Body = {
    bodyType match {
      case Text => new Body().withText(new Content().withCharset("UTF-8").withData(body))
      case Html =>
        new Body().withHtml(
          new Content().withCharset("UTF-8").withData(textToHtmlEmailBody(body)))
      case _ => throw new UnsupportedOperationException
    }
  }

  def main(args: Array[String]): Unit = {
    EmailSender.send(
      Set("akayyoor@netflix.com"),
      "Test Email",
      "Test email body",
      EmailSender.Html)
  }
}
