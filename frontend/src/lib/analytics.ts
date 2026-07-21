/**
 * PostHog Analytics Utility
 * Event tracking for common user activities
 */

import posthog from 'posthog-js';

// Signup & Authentication

export function trackSignupInitiated(properties?: { email?: string }) {
  posthog.capture('signup_initiated', properties);
}

export function trackSignupEmailSubmitted(properties?: { email?: string }) {
  posthog.capture('signup_email_submitted', properties);
}

export function trackActivationCodeSubmitted(properties?: { email?: string }) {
  posthog.capture('activation_code_submitted', properties);
}

export function trackAccountActivated(properties?: { email?: string; via_link?: boolean }) {
  posthog.capture('account_activated', properties);
}

export function trackLoginSuccess(properties?: { email?: string }) {
  posthog.capture('login_success', properties);
}

// Kafka Credentials

export function trackKafkaCredentialCreateInitiated(properties?: { credential_name?: string }) {
  posthog.capture('kafka_credential_create_initiated', properties);
}

export function trackKafkaCredentialCreated(properties?: { credential_id?: string; credential_name?: string }) {
  posthog.capture('kafka_credential_created', properties);
}

export function trackKafkaCredentialDeleted(properties?: { credential_id?: string }) {
  posthog.capture('kafka_credential_deleted', properties);
}

export function trackCredentialSecretToggled(properties?: { credential_id?: string; revealed?: boolean }) {
  posthog.capture('credential_secret_toggled', properties);
}

export function trackCredentialCopied(properties?: { label?: string }) {
  posthog.capture('credential_copied', properties);
}

// API Tokens

export function trackApiTokenCreateInitiated(properties?: { token_name?: string; expiry_days?: number }) {
  posthog.capture('api_token_create_initiated', properties);
}

export function trackApiTokenCreated(properties?: { token_id?: string; token_name?: string; expiry_days?: number }) {
  posthog.capture('api_token_created', properties);
}

export function trackApiTokenDeleted(properties?: { token_id?: string }) {
  posthog.capture('api_token_deleted', properties);
}

export function trackTokenCopied(properties?: { label?: string }) {
  posthog.capture('token_copied', properties);
}

// Search & Queries

export function trackAlertSearchSubmitted(properties?: Record<string, unknown>) {
  posthog.capture('alert_search_submitted', properties);
}

export function trackAlertSearchCompleted(properties?: Record<string, unknown>) {
  posthog.capture('alert_search_completed', properties);
}

export function trackObjectSearchSubmitted(properties?: Record<string, unknown>) {
  posthog.capture('object_search_submitted', properties);
}

export function trackObjectSearchCompleted(properties?: Record<string, unknown>) {
  posthog.capture('object_search_completed', properties);
}

/**
 * Track an error that occurred in the application
 */
export function trackError(
  context: string,
  error: unknown,
  additionalInfo?: Record<string, unknown>
) {
  const errorMessage = error instanceof Error ? error.message : String(error);
  posthog.capture('error_occurred', {
    category: 'error',
    context,
    error_message: errorMessage,
    ...additionalInfo,
  });
}

/**
 * Set user properties after signup/login
 */
export function setUserProperties(properties: Record<string, unknown>) {
  posthog.setPersonProperties(properties);
}

/**
 * Identify user by email or ID
 */
export function identifyUser(userId: string, email?: string) {
  posthog.identify(userId, {
    email,
  });
}

/**
 * Reset user identity on logout
 */
export function resetUser() {
  posthog.reset();
}
