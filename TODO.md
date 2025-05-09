# Capacity Market Search - General TODO List

This document contains high-priority tasks and issues that need to be addressed.

## Authentication and User Management

- **Fix Password Change Form**
  - Investigate why password change form shows "The two password fields didn't match" even when passwords appear to match
  - Implement better error reporting and debugging for password change form
  - Options:
    - Create a custom PasswordChangeView with detailed error logging
    - Improve form template to show more detailed error messages
    - Add more user guidance about password requirements

- **Email Confirmation (Partially Implemented)**
  - Configure production email backend (SendGrid, Mailgun) with proper API keys
  - Set DEFAULT_FROM_EMAIL in settings.py
  - Test full registration flow in production

- **Password Reset**
  - Create templates for Django's built-in password reset views
  - Configure email backend (same as confirmation)

## Search Functionality 

- **Improve Fuzzy Search Accuracy**
  - Tune RapidFuzz parameters for better matches
  - Implement field-specific strategies for postcodes, location names, and company names
  - Consider using exact matching for postcodes

- **Persist Sort Order Across Search Result Pages**
  - Modify pagination to maintain sort parameters
  - Update backend view to respect sort parameters

## Map Optimizations

- **Map API Performance**
  - Monitor cache effectiveness in production
  - Consider Redis for production if database cache becomes a bottleneck

- **Progressive Detail Loading Refinements**
  - Improve error handling for detail API calls
  - Consider pre-fetching details for nearby markers (optional)

## Frontend Improvements

- **Mobile Responsiveness**
  - Test all views on mobile devices
  - Improve touch controls for map on mobile
  - Ensure forms are properly sized on small screens

- **Accessibility**
  - Add proper ARIA attributes
  - Ensure color contrast meets WCAG standards
  - Test with screen readers

## Deployment and DevOps

- **CI/CD Pipeline**
  - Set up automated testing
  - Configure deployment checks

- **Monitoring**
  - Implement error tracking (Sentry)
  - Add performance monitoring
  - Set up alerts for critical errors

## Bug Fixes

- **Logout Error**
  - Fix 405 Method Not Allowed error when logging out from certain pages
  - Ensure all logout links use POST method with csrf_token

- **Search Results Display**
  - Fix blank results page when search returns no matches
  - Improve error messages for failed searches 

# TODO: Active/Inactive Status Implementation for Component Search

## Implementation Plan

### 1. Modify Group_By_Location Function
Enhance the `group_by_location` function in `checker_tags.py` to evaluate auction years for each location group. The function will add an `active_status` field to each group by examining all auction names in the group. For any location with auction years from 2024-25 and beyond, it will be marked as "Active"; otherwise, it will be "In-Active". This requires parsing the auction names to extract and compare the year portions.

### 2. Year Parsing and Evaluation Logic
Implement a utility function to determine if an auction year is "current" (2024-25 or later). This will extend the existing logic that handles auction years, using regex to extract the year component from auction names. The function will handle various formats like "2024-25", "2024/25", etc., and set the threshold at 2024-25 for the active status.

### 3. Template Updates for Displaying Status
Update the `search.html` template to display the active status as a prominent badge at the top of each component card. Use a green badge for "Active" components and a gray or red badge for "In-Active" ones. This will provide users with immediate visual feedback about a component's status without needing to scan through all auction years.

### 4. Styling and UX Considerations
Make the active status badge visually distinct from other badges but consistent with the site's design language. Position it prominently, possibly at the top right corner of the component card or at the beginning of the component information. The status should be immediately apparent to users scanning through search results.

### 5. Fix Inconsistency in Active/Inactive Threshold Across Application
There's currently an inconsistency in how active/inactive status is determined across different parts of the application:
- Search page: Uses 2024-25 as the cutoff (components from 2024-25 onward are "Active")
- Map and Statistics pages: Use 2025 as the cutoff (components from 2025 onward are "Active")

This needs to be unified. Update the map_data_api and statistics views to use 2024-25 as the threshold to match the search implementation.

### 6. Fix Stats Page Active/Inactive Classification
The stats page incorrectly classifies some components as inactive when they should be active. This happens because:
- Components at the same physical location but with slight differences in description are treated as different entities
- If any version of a component at a given location has auction years 2024-25 or later, all versions at that location should be considered "Active"

Update the stats page to determine active/inactive status based purely on location: "If any component with this location has auction years 2024-25 and beyond, mark as active."

### 7. Future Enhancement: Filtering by Status
In a future enhancement, add a filter UI element that allows users to show only Active or In-Active components. This would involve adding filter parameters to the search URL and modifying the search service to filter components based on this parameter. The filter UI could be implemented as radio buttons or a dropdown menu in the search form. 