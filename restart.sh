#!/bin/bash

# 設置腳本目錄
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# 顏色定義
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 日誌函數
log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

log_debug() {
    echo -e "${BLUE}[DEBUG]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

# 顯示幫助信息
show_help() {
    echo "用法: $0 [service_name]"
    echo ""
    echo "參數:"
    echo "  service_name    要重啟的服務名稱（可選）"
    echo ""
    echo "可用的服務:"
    echo "  smart_money_bsc"
    echo "  daily_update_bsc_smart_money"
    echo "  bsc_transaction_sync"
    echo ""
    echo "示例:"
    echo "  $0                    # 重啟所有服務"
    echo "  $0 bsc_transaction_sync    # 只重啟 bsc_transaction_sync"
    echo "  $0 smart_money_bsc         # 只重啟 smart_money_bsc"
}

# 檢查服務是否存在
check_service_exists() {
    local service_name="$1"
    local services=("smart_money_bsc" "daily_update_bsc_smart_money" "bsc_transaction_sync")
    
    for service in "${services[@]}"; do
        if [[ "$service" == "$service_name" ]]; then
            return 0
        fi
    done
    return 1
}

# 檢查服務狀態
check_service_status() {
    local service_name="$1"
    local status_output=$(supervisorctl status "$service_name" 2>/dev/null)
    
    if echo "$status_output" | grep -q "RUNNING"; then
        return 0  # 服務正在運行
    else
        return 1  # 服務未運行或狀態異常
    fi
}

# 執行 supervisorctl 命令並返回結果
execute_supervisorctl() {
    local command="$1"
    local service_name="$2"
    
    log_debug "執行命令: supervisorctl $command $service_name"
    
    # 執行命令並捕獲輸出
    local output
    if output=$(supervisorctl "$command" "$service_name" 2>&1); then
        return 0
    else
        echo "$output" >&2
        return 1
    fi
}

# 重啟單個服務
restart_single_service() {
    local service_name="$1"
    local stop_success=false
    local start_success=false
    
    if ! check_service_exists "$service_name"; then
        log_error "服務 '$service_name' 不存在"
        echo "可用的服務: smart_money_bsc, daily_update_bsc_smart_money, bsc_transaction_sync"
        return 1
    fi
    
    log_info "正在重啟服務: $service_name"
    
    # 先停止服務
    log_info "停止服務: $service_name"
    if execute_supervisorctl "stop" "$service_name"; then
        log_success "服務停止成功: $service_name"
        stop_success=true
    else
        log_error "服務停止失敗: $service_name"
        stop_success=false
    fi
    
    # 等待一秒
    sleep 1
    
    # 啟動服務
    log_info "啟動服務: $service_name"
    if execute_supervisorctl "start" "$service_name"; then
        log_success "服務啟動成功: $service_name"
        start_success=true
    else
        log_error "服務啟動失敗: $service_name"
        start_success=false
    fi
    
    # 檢查服務狀態
    sleep 2
    log_info "檢查服務狀態: $service_name"
    echo "----------------------------------------"
    supervisorctl status "$service_name"
    echo "----------------------------------------"
    
    # 最終狀態檢查
    if check_service_status "$service_name"; then
        log_success "服務 $service_name 重啟成功並正在運行"
        return 0
    else
        log_error "服務 $service_name 重啟失敗或狀態異常"
        return 1
    fi
}

# 重啟所有服務
restart_all_services() {
    local services=("smart_money_bsc" "daily_update_bsc_smart_money" "bsc_transaction_sync")
    local failed_services=()
    local success_services=()
    
    log_info "正在重啟所有服務..."
    echo ""
    
    for service in "${services[@]}"; do
        log_info "重啟服務: $service"
        if restart_single_service "$service"; then
            success_services+=("$service")
            log_success "服務 $service 重啟成功"
        else
            failed_services+=("$service")
            log_error "服務 $service 重啟失敗"
        fi
        echo ""  # 空行分隔
    done
    
    # 顯示最終結果
    echo "========================================"
    log_info "重啟結果總結:"
    echo "========================================"
    
    if [[ ${#success_services[@]} -eq ${#services[@]} ]]; then
        log_success "所有服務重啟成功！"
        log_success "smart_money_bsc 重啟成功"
    else
        log_error "部分服務重啟失敗"
        echo ""
        if [[ ${#success_services[@]} -gt 0 ]]; then
            log_success "成功重啟的服務:"
            for service in "${success_services[@]}"; do
                echo "  ✓ $service"
            done
        fi
        
        if [[ ${#failed_services[@]} -gt 0 ]]; then
            log_error "重啟失敗的服務:"
            for service in "${failed_services[@]}"; do
                echo "  ✗ $service"
            done
        fi
    fi
    
    echo ""
    log_info "BSC 服務狀態:"
    echo "----------------------------------------"
    supervisorctl status smart_money_bsc daily_update_bsc_smart_money bsc_transaction_sync
    echo "----------------------------------------"
    
    # 返回失敗的服務數量作為退出碼
    return ${#failed_services[@]}
}

# 主函數
main() {
    local service_name="$1"
    
    # 如果沒有參數，重啟所有服務
    if [[ -z "$service_name" ]]; then
        restart_all_services
        exit $?
    fi
    
    # 檢查是否是幫助命令
    if [[ "$service_name" == "help" || "$service_name" == "-h" || "$service_name" == "--help" ]]; then
        show_help
        exit 0
    fi
    
    # 重啟指定服務
    if restart_single_service "$service_name"; then
        log_success "服務 $service_name 重啟成功"
        exit 0
    else
        log_error "服務 $service_name 重啟失敗"
        exit 1
    fi
}

# 執行主函數
main "$@" 