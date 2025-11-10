local RatedStats, Achiev = ...
local playerName   = UnitName("player") .. "-" .. GetRealmName()

local f = CreateFrame("Frame")
f:RegisterEvent("ADDON_LOADED")
f:SetScript("OnEvent", function(self, event, addonName)
    if addonName ~= "RatedStats" then return end

    -- now it's safe to grab the namespace
    local RSTATS = _G.RSTATS

    if RSTATS then
        RSTATS:RegisterModule("Achiev", "Achievement Tooltip Enhancements", true)
    end

    -- we don't need this frame any more
    self:UnregisterEvent("ADDON_LOADED")
end)

-- Determine the region and use the appropriate achievement data
local regionMap = {
    [1] = "US",
    [2] = "KR",
    [3] = "EU",
    [4] = "TW",
}

local regionID = GetCurrentRegion()
local regionCode = regionMap[regionID] or "US"

-- Merge monolithic or chunked achievement files into one table
local function mergeRegionParts(region)
    local merged = {}
    local baseName = "ACHIEVEMENTS_" .. region
    local base = _G[baseName]
    if type(base) == "table" then
        for _, v in ipairs(base) do
            table.insert(merged, v)
        end
    end

    local partIndex = 1
    while true do
        local part = _G[baseName .. "_PART" .. partIndex]
        if type(part) ~= "table" then break end
        for _, v in ipairs(part) do
            table.insert(merged, v)
        end
        partIndex = partIndex + 1
    end

    return merged
end

local regionData = mergeRegionParts(regionCode)

-- Cache table to avoid repeat lookups
local achievementCache = {}

local R1Titles = {
    "Primal Gladiator", "Wild Gladiator", "Warmongering Gladiator",
    "Vindictive Gladiator", "Fearless Gladiator", "Cruel Gladiator",
    "Ferocious Gladiator", "Fierce Gladiator", "Demonic Gladiator",
    "Dread Gladiator", "Sinister Gladiator", "Notorious Gladiator",
    "Corrupted Gladiator", "Sinful Gladiator", "Unchained Gladiator",
    "Cosmic Gladiator", "Eternal Gladiator", "Crimson Gladiator",
    "Obsidian Gladiator", "Draconic Gladiator", "Seasoned Gladiator",
    "Forged Warlord", "Forged Marshal", "Forged Legend", "Forged Gladiator",
    "Prized Warlord", "Prized Marshal", "Prized Legend", "Prized Gladiator"
}

local HeroTitles = {
    "Hero of the Horde",
    "Hero of the Alliance"
}

local PvpRankColumns = {
    { key = "a", label = "Co-I",   prefix = "Combatant I",  icon = "Interface\\PVPFrame\\Icons\\UI_RankedPvP_01_Small.blp" },
    { key = "b", label = "Co-II",  prefix = "Combatant II", icon = "Interface\\PVPFrame\\Icons\\UI_RankedPvP_02_Small.blp" },
    { key = "c", label = "Ch-I",   prefix = "Challenger I", icon = "Interface\\PVPFrame\\Icons\\UI_RankedPvP_03_Small.blp" },
    { key = "d", label = "Ch-II",  prefix = "Challenger II",icon = "Interface\\PVPFrame\\Icons\\UI_RankedPvP_04_Small.blp" },
    { key = "e", label = "R-I",    prefix = "Rival I",      icon = "Interface\\PVPFrame\\Icons\\UI_RankedPvP_05_Small.blp" },
    { key = "f", label = "R-II",   prefix = "Rival II",     icon = "Interface\\PVPFrame\\Icons\\UI_RankedPvP_06_Small.blp" },
    { key = "g", label = "Duel",   prefix = "Duelist",      icon = "Interface\\PVPFrame\\Icons\\UI_RankedPvP_07_Small.blp" },
    { key = "h", label = "Elite",  prefix = "Gladiator",    icon = "Interface\\Icons\\Achievement_FeatsOfStrength_Gladiator_03.blp" },
    { key = "i", label = "Glad",   prefix = "Elite",        icon = "Interface\\Icons\\Achievement_FeatsOfStrength_Gladiator_07.blp" },
    { key = "j", label = "Legend", prefix = "Legend:",      icon = "Interface\\Icons\\Achievement_FeatsOfStrength_Gladiator_08.blp" },
    { key = "k", label = "Rank 1", r1 = true,               icon = "Interface\\Icons\\Achievement_FeatsOfStrength_Gladiator_08.blp" },
    { key = "l", label = "HotX",   hero = true,             icons = {
        "Interface\\PvPRankBadges\\PvPRankHorde.blp",
        "Interface\\PvPRankBadges\\PvPRankAlliance.blp"
    }},
}

-- Center text in fixed-width column
local function centerText(text, width)
    local str = tostring(text)
    local len = #str
    local pad = math.max(0, math.floor((width - len) / 2))
    return string.rep(" ", pad) .. str .. string.rep(" ", width - len - pad)
end

local function GetPvpAchievementSummary(entry)
    local summary = {}
    local highestRank = nil
    local highestRankIndex = 0

    for _, col in ipairs(PvpRankColumns) do
        summary[col.key] = 0
    end

    for _, val in pairs(entry) do
        if type(val) == "string" then
            local name = val:lower()

            for i, col in ipairs(PvpRankColumns) do
                if col.prefix and name:find(col.prefix:lower(), 1, true) then
                    summary[col.key] = summary[col.key] + 1
                    if i > highestRankIndex then
                        highestRank = val  -- ✅ store actual match string
                        highestRankIndex = i
                    end
                elseif col.r1 then
                    for _, r1 in ipairs(R1Titles) do
                        if name:find(r1:lower(), 1, true) then
                            summary[col.key] = summary[col.key] + 1
                            if i > highestRankIndex then
                                highestRank = r1
                                highestRankIndex = i
                            end
                        end
                    end
                elseif col.hero then
                    for _, hero in ipairs(HeroTitles) do
                        if name:find(hero:lower(), 1, true) then
                            summary[col.key] = summary[col.key] + 1
                            if i > highestRankIndex then
                                highestRank = hero
                                highestRankIndex = i
                            end
                        end
                    end
                end
            end
        end
    end

    return { summary = summary, highest = highestRank }
end

local function centerIcon(iconTag, width)
    local len = 3 -- 3 visual units for the icon
    local pad = math.max(0, math.floor((width - len) / 2))
    return string.rep(" ", pad) .. iconTag .. string.rep(" ", width - len - pad)
end

local function AddAchievementInfoToTooltip(tooltip, overrideName, overrideRealm)
    local _, unit = tooltip:GetUnit()
    local name, realm

    if unit and UnitIsPlayer(unit) then
        name, realm = UnitFullName(unit)
    else
        name, realm = overrideName, overrideRealm
    end

    if not name then return end
    realm = realm or GetRealmName()
    local key = (name .. "-" .. realm):lower()

    -- Avoid adding twice for same target, but allow refreshes
    if tooltip.__RatedStatsLast == key then return end
    tooltip.__RatedStatsLast = key

    tooltip:HookScript("OnHide", function(tip)
        tip.__RatedStatsLast = nil
    end)
    -- look up our per-char database and bail out if Achiev is off
    local key = UnitName("player") .. "-" .. GetRealmName()
    local db  = RSTATS.Database[key]
	local module = "RatedStats_Achiev"
    if C_AddOns.GetAddOnEnableState(module, nil) == 0 then
        return
    end
  
    local baseName, realm

    -- Use override only if tooltip:GetUnit() is not supported or not a unit tooltip
    local unit
    if tooltip.GetUnit then
        _, unit = tooltip:GetUnit()
    end

    if unit and UnitIsPlayer(unit) then
        baseName, realm = UnitFullName(unit)
    elseif overrideName then
        baseName = overrideName
        realm = overrideRealm or GetRealmName()
    else
        return -- No usable name/realm source
    end

    realm = realm or GetRealmName()
    local fullName = (baseName .. "-" .. realm:gsub("%s+", "")):lower()

    -- Cache lookup
    if achievementCache[fullName] == nil then
        local found = false
        for _, entry in ipairs(regionData) do
            if entry.character and entry.character:lower() == fullName then
                achievementCache[fullName] = GetPvpAchievementSummary(entry)
                found = true
                break
            end
        end
        if not found then
            print("Not found in data:", fullName)
            achievementCache[fullName] = { summary = {}, highest = nil }
        end
    end

	local result = achievementCache[fullName]
	local summary = result.summary or {}
	local highest = result.highest

    tooltip:AddLine("|cffffff00Rated Stats - Achievements|r")
    tooltip:AddLine("----------------------------")

    local hasAnyHistory = false
    for _, col in ipairs(PvpRankColumns) do
        if summary[col.key] and summary[col.key] > 0 then
            hasAnyHistory = true
            break
        end
    end
	
    if highest then
        tooltip:AddLine("|cff00ff00Highest PvP Rank:|r " .. highest)
    else
        tooltip:AddLine("|cffff0000No History / Not Seen in Bracket|r")
    end

	if hasAnyHistory then
		local iconRow, valueRow = "", ""
		local iconSize = 16
		local iconOffsetY = 0
	
		for _, col in ipairs(PvpRankColumns) do
			local count = summary[col.key] or 0
	
			-- Handle HotX (double icon column)
			if col.hero and col.icons then
				local icons = ""
				for _, iconPath in ipairs(col.icons) do
					icons = icons .. string.format("|T%s:%d:%d:0:%d|t", iconPath, iconSize, iconSize, iconOffsetY)
				end
				iconRow = iconRow .. centerIcon(icons, 10)
				valueRow = valueRow .. centerText(count, 12)
	
			else
				local iconTag = string.format("|T%s:%d:%d:0:%d|t", col.icon or "Interface\\Icons\\inv_misc_questionmark", iconSize, iconSize, iconOffsetY)
				iconRow = iconRow .. centerIcon(iconTag, 6)
				valueRow = valueRow .. centerText(count, 6)
			end
		end
	
		tooltip:AddLine(iconRow)
		tooltip:AddLine(valueRow)
	end

    tooltip:Show()
end

-- Minimal ScrollBoxUtil helper (mirrors Raider.IO core.lua)
local ScrollBoxUtil = {}

function ScrollBoxUtil:OnViewFramesChanged(scrollBox, callback)
    if not scrollBox then return end
    if scrollBox.GetFrames then
        local frames = scrollBox:GetFrames()
        if frames then
            callback(frames, scrollBox)
        end
        scrollBox:RegisterCallback(ScrollBoxListMixin.Event.OnUpdate, function()
            local updated = scrollBox:GetFrames()
            if updated then
                callback(updated, scrollBox)
            end
        end)
    end
end

local lastTooltipUnit = nil

-- Defer hook until player is fully in the game
local f = CreateFrame("Frame")
f:RegisterEvent("PLAYER_LOGIN")
f:RegisterEvent("UPDATE_MOUSEOVER_UNIT")

f:SetScript("OnEvent", function(_, event)
    if event == "PLAYER_LOGIN" then

        -- Hook GameTooltip:SetUnit for all players (including yourself)
        hooksecurefunc(GameTooltip, "SetUnit", function(tooltip)
            local _, unit = tooltip:GetUnit()
            if not unit or not UnitIsPlayer(unit) then return end

            local name, realm = UnitFullName(unit)
            realm = realm or GetRealmName()
            local key = (name .. "-" .. realm):lower()

            -- For yourself: always allow reinjection (no cache skip)
            if unit ~= "player" and tooltip.__RatedStatsLast == key then
                return
            end
            tooltip.__RatedStatsLast = key

            C_Timer.After(0.05, function()
                if tooltip:IsShown() and UnitIsPlayer(unit) then
                    AddAchievementInfoToTooltip(tooltip, name, realm)
                end
            end)
        end)

        -- Fallback: handle when hovering your own player frame (for UIs that don't call SetUnit)
        local playerFrame = PlayerFrame or ElvUF_Player or SUFUnitplayer
        if playerFrame and not playerFrame.__RatedStatsHooked then
            playerFrame.__RatedStatsHooked = true
            playerFrame:HookScript("OnEnter", function()
                local name, realm = UnitFullName("player")
                realm = realm or GetRealmName()
                C_Timer.After(0.05, function()
                    if GameTooltip:IsShown() then
                        AddAchievementInfoToTooltip(GameTooltip, name, realm)
                    end
                end)
            end)
        end

		-- Hook UnitFrame mouseovers (party/raid frames etc.)
		hooksecurefunc("UnitFrame_OnEnter", function(self)
			if not self or not self.unit or not UnitIsPlayer(self.unit) then return end
			local name, realm = UnitFullName(self.unit)
			realm = realm or GetRealmName()
		
			-- Delay a touch to ensure tooltip lines are added
			C_Timer.After(0.05, function()
				if GameTooltip:IsShown() then
					AddAchievementInfoToTooltip(GameTooltip, name, realm)
				end
			end)
		end)

        -- Hook LFG tooltips
        hooksecurefunc("LFGListUtil_SetSearchEntryTooltip", function(tooltip, resultID)
            local _, _, name, _, _, _, _, _, _, _, _, leaderName = C_LFGList.GetSearchResultInfo(resultID)
            if leaderName then
                local realm = GetNormalizedRealmName() or GetRealmName()

                -- Delay to ensure other tooltip extensions (e.g., RaiderIO) have run
                C_Timer.After(0.05, function()
                    if tooltip and tooltip:IsShown() then
                        -- Ensure correct anchor if needed
                        if not tooltip:GetOwner() then
                            tooltip:SetOwner(UIParent, "ANCHOR_CURSOR")
                        end
                        tooltip:ClearLines()
                        -- Append your achievement info
                        AddAchievementInfoToTooltip(tooltip, leaderName, realm)
                    end
                end)
            end
        end)

        -- Hook applicant tooltip popout (like Raider.IO)
        local function TryHookApplicantTooltip()
            local mixin = _G.TooltipLFGApplicantMixin
            if type(mixin) == "table" and mixin.SetApplicantMember then
                hooksecurefunc(mixin, "SetApplicantMember", function(self, applicantID, memberIdx)
                    C_Timer.After(0.05, function()
                        print("RatedStats: Applicant popout hook fired: applicantID=", applicantID, "memberIdx=", memberIdx)
                        local name, class, localizedClass, level, itemLevel, tank, healer, damage, assignedRole, relationship =
                            C_LFGList.GetApplicantMemberInfo(applicantID, memberIdx)
                        if not name then
                            print("RatedStats: Missing applicant member info for index", memberIdx)
                            return
                        end

                        local baseName, realm = strsplit("-", name)
                        realm = realm or GetRealmName()
                        print(string.format("RatedStats: Applicant %s-%s (appID=%d idx=%d)", baseName or "?", realm or "?", applicantID, memberIdx))
                        AddAchievementInfoToTooltip(self, baseName, realm)
                    end)
                end)
                print("RatedStats: TooltipLFGApplicantMixin hook attached.")
                return true
            end
            return false
        end

        -- Keep retrying until the mixin exists
        local function WaitForMixin()
            if not TryHookApplicantTooltip() then
                C_Timer.After(0.05, WaitForMixin)
            end
        end
        C_Timer.After(0.05, WaitForMixin)

        -- Hook CommunitiesFrame (Guild Roster) ScrollBox row tooltips
        local function HookCommunitiesGuildRows()
            local container = CommunitiesFrame and CommunitiesFrame.MemberList and CommunitiesFrame.MemberList.ScrollBox
            if not container then return end

            local function HookRow(frame)
                if frame.__ratedStatsHooked then return end
                frame.__ratedStatsHooked = true

                frame:HookScript("OnEnter", function(self)
                    local info = self.memberInfo
                    if not info or not info.name then return end

                    local name, realm = strsplit("-", info.name)
                    realm = realm or GetRealmName()
                    AddAchievementInfoToTooltip(GameTooltip, name, realm)
                end)
            end

            container:RegisterCallback("OnAcquiredFrame", function(_, frame)
                if type(frame) == "table" and frame.GetObjectType then
                    HookRow(frame)
                end
            end, true)
        end

        C_Timer.After(0.05, HookCommunitiesGuildRows)

        -- Hook applicant rows in LFG
        local function HookApplicantFrames()
            local scrollBox = LFGListFrame and LFGListFrame.ApplicationViewer and LFGListFrame.ApplicationViewer.ScrollBox
            if not scrollBox or not scrollBox.GetFrames then
                C_Timer.After(0.05, HookApplicantFrames)
                return
            end

            local hooked = {}
            local function OnEnter(self)
                if self.applicantID and self.Members then
                    for _, member in pairs(self.Members) do
                        if not hooked[member] then
                            hooked[member] = true
                            member:HookScript("OnEnter", function(memberFrame)
                                local name, realm = strsplit("-", memberFrame.memberName or "")
                                if name then
                                    realm = realm or GetRealmName()
                                    AddAchievementInfoToTooltip(GameTooltip, name, realm)
                                end
                            end)
                            member:HookScript("OnLeave", function() GameTooltip:Hide() end)
                        end
                    end
                elseif self.memberIdx then
                    local parent = self:GetParent()
                    local memberName = select(1, C_LFGList.GetApplicantMemberInfo(parent.applicantID, self.memberIdx))
                    if memberName then
                        local baseName, realm = strsplit("-", memberName)
                        realm = realm or GetRealmName()
                        AddAchievementInfoToTooltip(GameTooltip, baseName, realm)
                    end
                end
            end

            local frames = scrollBox:GetFrames()
            if not frames or #frames == 0 then
                C_Timer.After(0.05, HookApplicantFrames)
                return
            end

            for _, frame in ipairs(frames) do
                if not hooked[frame] then
                    hooked[frame] = true
                    frame:HookScript("OnEnter", OnEnter)
                    frame:HookScript("OnLeave", function() GameTooltip:Hide() end)
                end
            end
        end

        C_Timer.After(0.05, HookApplicantFrames)

    elseif event == "UPDATE_MOUSEOVER_UNIT" then
        if UnitIsPlayer("mouseover") then
            local name, realm = UnitFullName("mouseover")
            realm = realm or GetRealmName()
            C_Timer.After(0.05, function()
                if GameTooltip:IsShown() then
                    AddAchievementInfoToTooltip(GameTooltip, name, realm)
                end
            end)
        end
    end
end) -- closes f:SetScript