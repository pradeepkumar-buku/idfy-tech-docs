  ### 📥 Overview

  The IDfy KYC webhook is the automated callback endpoint triggered by IDfy when a user finishes their identity verification journey (scanning KTP/DL, capturing
  selfie, OCR, and liveness checks).
  ──────
  ### ⚙️ Step-by-Step Processing Flow

  #### 1. Ingestion & Logging (IdfyCallbackController.java)

  • Checks feature flag idfy.callback.enabled (if false, returns 503 Service Unavailable).
  • Sets MDC logging context (journeyId, clientReferenceId, decision).
  • Increments Datadog metrics (janus.webhook.received_count).

  #### 2. Check Retrieval & Terminal Status Check (ProcessIdfyKycWebhookService.java)

  • Looks up the corresponding IdyfyKycCheck record in staging.checks using journeyId (profileId).
  • If no check exists yet, throws an error so Kafka/SQS consumer routes the payload to unprocessed_event_messages for retry.
  • If the check is already in a terminal state (VERIFIED, REJECTED, MANUALLY_VERIFIED), it skips duplicate processing.

  #### 3. Image Downloads (IdyfyImageDownloadService.java)

  Before starting the DB transaction, it parses the temporary IDfy CDN URLs in tasks and uploads images to Google Cloud Storage (GCS):

  • KTP Front: tasks.ktp.sides.front.url → saved to GCS under attachments/ktp/ → row inserted into staging.attachments with attachment_type = 'KTP'.
  • Driver's License (DL): tasks.dl.sides.front.url → saved to GCS under attachments/additional/driving-license/ → row inserted into staging.attachments with
  attachment_type = 'DL'.
  • Selfie: tasks.selfie.url → saved to GCS under attachments/selfie/ → row inserted into staging.attachments with attachment_type = 'SELFIE'.

  #### 4. Decision Mapping (IdfyDecisionMapper.java)

  Maps the IDfy provider decision payload string to Janus internal statuses:

   IDfy Decision Payload                 | Janus Check Status                    | Janus Account KYC Status              | Action
  ---------------------------------------|---------------------------------------|---------------------------------------|------------------------------------------
   "APPROVED"                            | VERIFIED                              | VERIFIED                              | Auto-approved. KYC complete.
   "INDETERMINATE"                       | PENDING_MANUAL_VERIFICATION           | PENDING_MANUAL_VERIFICATION           | Sent to Ops Webportal for manual review.
   "REJECTED"                            | REJECTED                              | REJECTED                              | Auto-rejected.

  #### 5. Database Updates (staging.checks & staging.accounts)

  Inside an atomic DB transaction:

  • staging.checks:
      • Saves the full IDfy task payload into details->'idfy_tasks' JSONB (OCR fields, tampering scores, face match scores).
      • Saves selfie score into details->'liveness_response'.
      • Updates check status column to VERIFIED, PENDING_MANUAL_VERIFICATION, or REJECTED.
  • staging.accounts:
      • Sets kyc_status and kyc_source = 'IDFY'.
      • Extracts OCR NIK & Name (ktp.automated_ocr_fields.nik/nama) and updates account_nik and account_name.
      • Sets completed_date if VERIFIED.
      • Recomputes kyc_tier:
          • Set to ADVANCED if KYC is VERIFIED.
          • Set to SUPREME if KYC is VERIFIED AND the user's KYB status is also VERIFIED.



  #### 6. Routing & Kafka Event Publishing

  • Updates routing status in staging.user_provider_routing.
  • Publishes KycProgressUpdateEvent to Kafka topic buku.janus.kyc-progress-update.v1 so downstream services (Payments, Banking, Rewards) are instantly notified of
  the user's updated KYC status and tier.
